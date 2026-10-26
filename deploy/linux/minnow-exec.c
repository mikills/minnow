// Apply the process memory policy before Go and native libraries initialize.
// No global kernel settings are changed. exec preserves PID, signals and argv.
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc < 2) {
        fputs("usage: minnow-exec executable [args...]\n", stderr);
        return 2;
    }
    const char *setting = getenv("MINNOW_DISABLE_THP");
    int disable = setting && (!strcmp(setting, "1") || !strcmp(setting, "true"));
    if (setting && *setting && !disable && strcmp(setting, "0") && strcmp(setting, "false")) {
        fputs("MINNOW_DISABLE_THP must be 1, true, 0, or false\n", stderr);
        return 2;
    }
    if (disable) {
        if (prctl(PR_SET_THP_DISABLE, 1L, 0L, 0L, 0L)) {
            perror("minnow-exec: disable transparent huge pages");
            return 1;
        }
        int state = prctl(PR_GET_THP_DISABLE, 0L, 0L, 0L, 0L);
        if (state < 0) {
            perror("minnow-exec: read transparent huge-page policy");
            return 1;
        }
        if (state != 1) {
            fputs("minnow-exec: transparent huge-page policy verification failed\n", stderr);
            return 1;
        }
    }
    // Opting out preserves the inherited policy; it does not enable THP that
    // a parent process or host administrator has disabled.
    execvp(argv[1], &argv[1]);
    int error = errno;
    perror("minnow-exec: exec");
    return error == ENOENT ? 127 : 126;
}
