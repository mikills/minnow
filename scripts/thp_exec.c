// CI-only launcher: change THP policy before exec, including Go/native startup.
// This does not change the runner's global policy or any production defaults.
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <unistd.h>

int main(int argc, char **argv) {
    const char *actions = getenv("GITHUB_ACTIONS");
    const char *runner = getenv("RUNNER_ENVIRONMENT");
    if (!actions || strcmp(actions, "true") || !runner || strcmp(runner, "github-hosted")) {
        fputs("THP diagnostics require a GitHub-hosted Actions runner\n", stderr);
        return 1;
    }
    if (argc < 3 || (strcmp(argv[1], "default") && strcmp(argv[1], "disabled"))) {
        fputs("usage: thp-exec default|disabled executable [args...]\n", stderr);
        return 1;
    }
    int before = prctl(PR_GET_THP_DISABLE, 0L, 0L, 0L, 0L);
    if (before != 0) {
        fprintf(stderr, "unexpected inherited THP disable state: %d\n", before);
        return 1;
    }
    int disabled = !strcmp(argv[1], "disabled");
    if (disabled && prctl(PR_SET_THP_DISABLE, 1L, 0L, 0L, 0L)) {
        perror("disable transparent huge pages");
        return 1;
    }
    if (prctl(PR_GET_THP_DISABLE, 0L, 0L, 0L, 0L) != disabled) {
        fputs("THP policy verification failed\n", stderr);
        return 1;
    }
    fprintf(stderr, "THP_DISABLE_BEFORE_EXEC=%d\n", disabled);
    fflush(stderr);
    execvp(argv[2], &argv[2]);
    perror("exec THP diagnostic");
    return 1;
}
