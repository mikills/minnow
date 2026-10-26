"""CI-only Linux launcher correctness tests; no database or performance workload."""

import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import tempfile
import unittest

from compare_duckdb import require_ci


PROBE = """
import json, os, sys, time
from pathlib import Path
status = dict(line.split(':', 1) for line in Path('/proc/self/status').read_text().splitlines())
print(json.dumps({'thp_enabled': int(status['THP_enabled']), 'pid': os.getpid(),
                  'args': sys.argv[1:], 'cwd': os.getcwd(), 'marker': os.getenv('LAUNCH_MARKER')}), flush=True)
if '--wait' in sys.argv: time.sleep(30)
"""


class LauncherTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        require_ci(os.environ)
        if sys.platform != "linux":
            raise RuntimeError("THP launcher checks require Linux")
        cls.directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.directory.cleanup)
        cls.root = Path(cls.directory.name)
        cls.launcher = cls.root / "minnow-exec"
        source = Path(__file__).resolve().parents[1] / "deploy/linux/minnow-exec.c"
        subprocess.run(["cc", "-O2", "-Wall", "-Wextra", "-Werror", str(source), "-o", str(cls.launcher)], check=True)
        # Inject syscall failures to ensure the requested policy fails closed.
        shim = cls.root / "prctl.c"
        shim.write_text('''#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
int prctl(int option, ...) {
    const char *stage = getenv("PRCTL_FAILURE");
    if (!strcmp(stage, "set") || (option == PR_GET_THP_DISABLE && !strcmp(stage, "get"))) {
        errno = EPERM;
        return -1;
    }
    return 0;
}
''')
        cls.shim = cls.root / "prctl.so"
        subprocess.run(["cc", "-shared", "-fPIC", "-Wall", "-Wextra", "-Werror", str(shim), "-o", str(cls.shim)], check=True)
        base = subprocess.check_output([sys.executable, "-c", PROBE], text=True)
        if json.loads(base)["thp_enabled"] != 1:
            raise RuntimeError("launcher test requires an initially THP-enabled process")

    def env(self, setting):
        env = dict(os.environ, LAUNCH_MARKER="preserved")
        env.pop("MINNOW_DISABLE_THP", None)
        if setting is not None:
            env["MINNOW_DISABLE_THP"] = setting
        return env

    def test_policy(self):
        args = ["space in argument", "$(not-a-shell-command)", "--flag=value"]
        for setting, enabled in ((None, 1), ("", 1), ("0", 1), ("false", 1), ("1", 0), ("true", 0)):
            with self.subTest(setting=setting):
                with subprocess.Popen([str(self.launcher), sys.executable, "-c", PROBE, *args],
                                      cwd=self.root, env=self.env(setting), stdout=subprocess.PIPE, text=True) as child:
                    output, _ = child.communicate(timeout=10)
                    self.assertEqual(child.returncode, 0)
                    self.assertEqual(json.loads(output), {"thp_enabled": enabled, "pid": child.pid,
                                                         "args": args, "cwd": str(self.root), "marker": "preserved"})

    def test_inheritance(self):
        # The second launcher must neither undo an inherited disable nor fail
        # when the requested disable is already applied.
        for setting in ("0", "1"):
            with self.subTest(setting=setting):
                output = subprocess.check_output([str(self.launcher), "env", f"MINNOW_DISABLE_THP={setting}",
                                                  str(self.launcher), sys.executable, "-c", PROBE],
                                                 env=self.env("1"), text=True, timeout=10)
                self.assertEqual(json.loads(output)["thp_enabled"], 0)

    def test_exit_and_signal(self):
        result = subprocess.run([str(self.launcher), sys.executable, "-c", "raise SystemExit(37)"],
                                env=self.env("1"), timeout=10)
        self.assertEqual(result.returncode, 37)
        with subprocess.Popen([str(self.launcher), sys.executable, "-c", PROBE, "--wait"],
                              env=self.env("1"), stdout=subprocess.PIPE, text=True) as child:
            try:
                with selectors.DefaultSelector() as ready:
                    ready.register(child.stdout, selectors.EVENT_READ)
                    self.assertTrue(ready.select(timeout=10), "probe never started")
                self.assertEqual(json.loads(child.stdout.readline())["pid"], child.pid)
                child.send_signal(signal.SIGTERM)
                child.communicate(timeout=10)
                self.assertEqual(child.returncode, -signal.SIGTERM)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait()

    def test_failures(self):
        for setting in ("yes", "typo", "2"):
            with self.subTest(setting=setting):
                result = subprocess.run([str(self.launcher), sys.executable, "-c", "print('executed')"],
                                        env=self.env(setting), capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
        for stage in ("set", "get", "verification"):
            with self.subTest(stage=stage):
                env = dict(self.env("1"), LD_PRELOAD=str(self.shim), PRCTL_FAILURE=stage)
                result = subprocess.run([str(self.launcher), sys.executable, "-c", "print('executed')"],
                                        env=env, capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout, "")
                self.assertIn("minnow-exec:", result.stderr)
        for args, code in (([], 2), ([str(self.root / "missing")], 127), ([str(self.root)], 126)):
            with self.subTest(args=args):
                result = subprocess.run([str(self.launcher), *args], env=self.env("1"), capture_output=True, timeout=10)
                self.assertEqual(result.returncode, code)


if __name__ == "__main__":
    unittest.main()
