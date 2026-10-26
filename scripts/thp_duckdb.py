"""CI-only 2x2 engine/THP attribution, separate from parser and throughput runs."""

import argparse
import json
import os
from pathlib import Path
import statistics

from compare_duckdb import CONTROLS, require_ci
from memory_duckdb import LABELS, MODES, VERSIONS, select_sample, validate as validate_memory
from parser_duckdb import paired_values
from profile_duckdb import run


POLICIES = ("default", "disabled")
RUNS = 3 * len(MODES) * len(LABELS) * len(POLICIES)


def schedule():
    for index in range(3):
        for position, mode in enumerate(MODES):
            labels = LABELS if (index + position) % 2 == 0 else tuple(reversed(LABELS))
            for label in labels:
                policies = POLICIES if (index + position + LABELS.index(label)) % 2 == 0 else tuple(reversed(POLICIES))
                for policy in policies:
                    yield index, mode, label, policy


def validate(data, mode, label, policy, engine):
    validate_memory(data, mode, VERSIONS[label])
    opens = 1 if mode == "ingest" else 20
    if (engine.get("version") != VERSIONS[label] or not engine.get("source")
            or data.get("source") != engine["source"] or data.get("error") or data.get("parser")
            or policy not in POLICIES or data.get("thp_policy") != policy or data.get("thp_checks") != 2 * opens
            or bool(data.get("thp_ingest_checked")) != (mode == "ingest")):
        raise RuntimeError("engine/THP/security/limit verification mismatch")
    for sample in data["samples"]:
        if type(sample.get("thp_disabled")) is not int or sample["thp_disabled"] != int(policy == "disabled"):
            raise RuntimeError("process did not retain the requested THP policy")
        huge = sample["os_kib"].get("AnonHugePages")
        if type(huge) is not int or huge < 0 or (policy == "disabled" and huge != 0):
            raise RuntimeError("missing/invalid AnonHugePages or huge pages present in disabled arm")
        for mapping in sample["mappings"].values():
            value = mapping.get("anon_huge_pages_kib")
            if type(value) is not int or value < 0 or (policy == "disabled" and value != 0):
                raise RuntimeError("missing/invalid mapping huge-page counters")


def pairs(records, mode, label):
    rows = [r for r in records if r["data"]["mode"] == mode and r["label"] == label]
    keyed = {(r["round"], r["policy"]): r["data"] for r in rows}
    expected = {(index, policy) for index in range(3) for policy in POLICIES}
    if len(rows) != 6 or set(keyed) != expected:
        return []
    return [(keyed[index, "default"], keyed[index, "disabled"]) for index in range(3)]


def report(records, errors, finished):
    lines = ["## DuckDB transparent huge-page attribution", "",
             "Same hosted runner; two released engines, each compared with itself under default versus disabled THP.",
             "Fresh sequential processes, three alternating paired rounds. THP is disabled before exec, including Go/native startup.",
             "Signed extensions, 128MB memory limit, one DuckDB thread; no parser or allocator tuning.",
             "Memory attribution/purge instrumentation is identical in both arms. Values below precede purge and trim.",
             "MiB medians; percentage changes are medians of within-round disabled/default changes.",
             "This is process-wide attribution, including Go: it does not isolate a DuckDB-only THP policy or measure throughput.", ""]
    if not finished or errors or len(records) != RUNS:
        lines += ["**Incomplete diagnostics: only complete three-round pairs are tabulated.**", ""]
    lines += ["| mode | engine | after close/GC counter | default MiB | disabled MiB | paired change |",
              "| --- | --- | --- | ---: | ---: | ---: |"]
    for mode in MODES:
        for label in LABELS:
            paired = pairs(records, mode, label)
            if not paired:
                continue
            for key in ("Rss", "Anonymous", "AnonHugePages", "Go-mapped RSS", "Go-mapped huge pages", "unclassified anonymous RSS"):
                def memory(data):
                    sample = select_sample(data, "after_gc", 0)
                    if key.startswith("Go-mapped"):
                        field = "rss_kib" if key == "Go-mapped RSS" else "anon_huge_pages_kib"
                        return sum(v[field] for k, v in sample["mappings"].items() if k.startswith("[anon: Go:")) / 1024
                    if key == "unclassified anonymous RSS":
                        return sample["mappings"].get("[anonymous]", {}).get("rss_kib", 0) / 1024
                    return sample["os_kib"][key] / 1024
                a, b, change = paired_values(paired, memory)
                lines.append(f"| {mode} | {VERSIONS[label]} | {key} | {a:.2f} | {b:.2f} | {change} |")
    lines += ["", "### Remaining engine RSS difference", "",
              "Median paired 2.0 minus 1.5 MiB after close/GC, under each fixed process policy.", "",
              "| mode | default THP | disabled THP |", "| --- | ---: | ---: |"]
    for mode in MODES:
        baseline, candidate = (pairs(records, mode, label) for label in LABELS)
        if not baseline or not candidate:
            continue
        deltas = [statistics.median((select_sample(b[p], "after_gc", 0)["os_kib"]["Rss"] -
                                     select_sample(a[p], "after_gc", 0)["os_kib"]["Rss"]) / 1024
                                    for a, b in zip(baseline, candidate)) for p in range(2)]
        lines.append(f"| {mode} | {deltas[0]:+.2f} | {deltas[1]:+.2f} |")
    lines += ["", "RSS is resident memory, not live allocations or proof of a leak. Huge-page and RSS counters overlap.",
              "smaps and smaps_rollup are sequential snapshots, so their totals need not match exactly.",
              "Raw JSON retains first-load/close snapshots, allocator statistics and per-path huge-page attribution.",
              "A production policy change would still need application CPU/latency and memory validation."]
    if errors:
        lines += ["", "### Failed or unavailable diagnostics"] + [f"- {error}" for error in errors]
    return "\n".join(lines) + "\n"


def collect(root, engines, env):
    require_ci(env)
    results = root / "thp"
    raw, records, errors = [], [], []
    finished = False

    def save():
        (results / "measurements.json").write_text(json.dumps({
            "engines": engines, "controls": CONTROLS, "runs": raw,
            "errors": errors, "finished": finished,
        }, indent=2) + "\n")
        text = report(records, errors, finished)
        (results / "summary.md").write_text(text)
        return text

    save()
    try:
        for index, mode, label, policy in schedule():
            name = f"{label}-{policy}-{mode}-{index}"
            prefix = results / name
            try:
                print(name, flush=True)
                home = results / f"{name}.home"
                home.mkdir()
                command = [str(results / "thp-exec"), policy, str(results / f"{label}-profile"),
                           "-mode", mode, "-iterations", "20", "-extensions", str(root / label / "extensions"),
                           "-expect", VERSIONS[label], "-memory-attribution", "-thp-policy", policy]
                run(command, prefix, root / label, dict(env, **CONTROLS, HOME=str(home)), timeout=120)
                data = json.loads(Path(str(prefix) + ".out").read_text())
                record = {"label": label, "policy": policy, "round": index, "data": data}
                raw.append(record)
                validate(data, mode, label, policy, engines[label])
                records.append(record)
            except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
                errors.append(f"{name}: {error}")
            finally:
                save()
        finished = True
    finally:
        text = save()
        with open(env["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(text)
        print(text)
    if errors or len(records) != RUNS:
        raise RuntimeError("THP comparison incomplete; inspect saved diagnostics")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    require_ci(os.environ)
    for key in ("MALLOC_CONF", "DUCKDB_JE_MALLOC_CONF", "LD_PRELOAD", "GLIBC_TUNABLES", "GODEBUG"):
        if os.environ.get(key):
            raise RuntimeError(f"unexpected memory/runtime override: {key}")
    root = args.root.resolve()
    engines = {label: json.loads((root / "thp" / f"{label}.engine.json").read_text()) for label in LABELS}
    if any(engine.get("version") != VERSIONS[label] or not engine.get("source") for label, engine in engines.items()):
        raise RuntimeError(f"expected released 1.5.0 and 2.0.0: {engines}")
    collect(root, engines, dict(os.environ))


if __name__ == "__main__":
    main()
