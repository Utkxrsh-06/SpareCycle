#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/poll.h>
#include <sys/prctl.h>
#include <time.h>
#include <unistd.h>

extern char **environ;

static double wall_seconds(const struct timespec *start) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (now.tv_sec - start->tv_sec) +
           (now.tv_nsec - start->tv_nsec) / 1000000000.0;
}

static const char *argument(int argc, char **argv, const char *name) {
    size_t name_length = strlen(name);
    for (int i = 1; i < argc - 1; ++i) {
        if (strncmp(argv[i], name, name_length) == 0 &&
            argv[i][name_length] == '=')
            return argv[i] + name_length + 1;
    }
    return NULL;
}

static void json_escape(FILE *out, const char *value) {
    fputc('"', out);
    for (const unsigned char *p = (const unsigned char *)value; *p; ++p) {
        switch (*p) {
            case '\\': fputs("\\\\", out); break;
            case '"': fputs("\\\"", out); break;
            case '\n': fputs("\\n", out); break;
            case '\r': fputs("\\r", out); break;
            case '\t': fputs("\\t", out); break;
            default:
                if (*p < 0x20) fprintf(out, "\\u%04x", *p);
                else fputc(*p, out);
        }
    }
    fputc('"', out);
}

static char *read_stdin(size_t *length) {
    size_t capacity = 4096;
    size_t used = 0;
    char *buffer = (char *)malloc(capacity);
    if (!buffer) return NULL;

    for (;;) {
        if (used == capacity) {
            capacity *= 2;
            char *grown = (char *)realloc(buffer, capacity);
            if (!grown) {
                free(buffer);
                return NULL;
            }
            buffer = grown;
        }
        ssize_t count = read(STDIN_FILENO, buffer + used, capacity - used);
        if (count > 0) {
            used += (size_t)count;
        } else if (count == 0) {
            break;
        } else if (errno == EINTR) {
            continue;
        } else {
            free(buffer);
            return NULL;
        }
    }
    *length = used;
    return buffer;
}

int main(int argc, char **argv) {
    const char *python_path = argument(argc, argv, "--python");
    const char *handler_path = argument(argc, argv, "--handler");
    const char *working_directory = argument(argc, argv, "--cwd");
    const char *job_id = argument(argc, argv, "--job-id");
    const char *timeout_text = argument(argc, argv, "--timeout");
    const char *memory_text = argument(argc, argv, "--memory-mb");
    const char *cpu_text = argument(argc, argv, "--cpu-seconds");

    if (!python_path || !handler_path || !working_directory || !timeout_text) {
        fprintf(stderr, "missing required native process-manager arguments\n");
        return 2;
    }

    double timeout = strtod(timeout_text, NULL);
    unsigned long memory_mb = memory_text ? strtoul(memory_text, NULL, 10) : 0;
    unsigned long cpu_seconds = cpu_text ? strtoul(cpu_text, NULL, 10) : 0;
    size_t request_length = 0;
    char *request = read_stdin(&request_length);
    if (!request) {
        fprintf(stderr, "could not read execution request\n");
        return 2;
    }

    int stdin_pipe[2], stdout_pipe[2], stderr_pipe[2];
    if (pipe(stdin_pipe) || pipe(stdout_pipe) || pipe(stderr_pipe)) {
        perror("pipe");
        free(request);
        return 2;
    }

    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    pid_t child = fork();
    if (child < 0) {
        perror("fork");
        free(request);
        return 2;
    }

    if (child == 0) {
        setsid();
        prctl(PR_SET_PDEATHSIG, SIGKILL);
        close(stdin_pipe[1]);
        close(stdout_pipe[0]);
        close(stderr_pipe[0]);
        dup2(stdin_pipe[0], STDIN_FILENO);
        dup2(stdout_pipe[1], STDOUT_FILENO);
        dup2(stderr_pipe[1], STDERR_FILENO);
        close(stdin_pipe[0]);
        close(stdout_pipe[1]);
        close(stderr_pipe[1]);

        if (chdir(working_directory) != 0) _exit(126);
        if (cpu_seconds > 0) {
            struct rlimit limit = {(rlim_t)cpu_seconds, (rlim_t)cpu_seconds};
            if (setrlimit(RLIMIT_CPU, &limit) != 0) _exit(125);
        }
        if (memory_mb > 0) {
            rlim_t bytes = (rlim_t)memory_mb * 1024 * 1024;
            struct rlimit limit = {bytes, bytes};
            if (setrlimit(RLIMIT_AS, &limit) != 0) _exit(124);
        }

        char *const child_argv[] = {
            (char *)python_path, "-u", (char *)handler_path, NULL
        };
        execve(python_path, child_argv, environ);
        _exit(127);
    }

    close(stdin_pipe[0]);
    close(stdout_pipe[1]);
    close(stderr_pipe[1]);
    fcntl(stdout_pipe[0], F_SETFL, O_NONBLOCK);
    fcntl(stderr_pipe[0], F_SETFL, O_NONBLOCK);

    size_t written = 0;
    fcntl(stdin_pipe[1], F_SETFL, O_NONBLOCK);
    while (written < request_length) {
        ssize_t count = write(stdin_pipe[1], request + written, request_length - written);
        if (count > 0) written += (size_t)count;
        else if (count < 0 && errno != EINTR && errno != EAGAIN) break;
        struct timespec small = {0, 1000000};
        nanosleep(&small, NULL);
    }
    close(stdin_pipe[1]);

    char stdout_buffer[65536] = {0};
    char stderr_buffer[65536] = {0};
    size_t stdout_used = 0, stderr_used = 0;
    int status = 0;
    struct rusage usage;
    memset(&usage, 0, sizeof(usage));
    pid_t waited = 0;
    bool timed_out = false;
    const char *termination_reason = "exit";

    while (waited == 0) {
        char temporary[4096];
        ssize_t count;
        while ((count = read(stdout_pipe[0], temporary, sizeof(temporary))) > 0) {
            size_t copy = (size_t)count;
            if (stdout_used + copy > sizeof(stdout_buffer) - 1) copy = sizeof(stdout_buffer) - 1 - stdout_used;
            if (copy > 0) memcpy(stdout_buffer + stdout_used, temporary, copy);
            stdout_used += copy;
        }
        while ((count = read(stderr_pipe[0], temporary, sizeof(temporary))) > 0) {
            size_t copy = (size_t)count;
            if (stderr_used + copy > sizeof(stderr_buffer) - 1) copy = sizeof(stderr_buffer) - 1 - stderr_used;
            if (copy > 0) memcpy(stderr_buffer + stderr_used, temporary, copy);
            stderr_used += copy;
        }

        waited = wait4(child, &status, WNOHANG, &usage);
        if (waited < 0) break;
        if (waited == 0 && wall_seconds(&start) >= timeout) {
            timed_out = true;
            termination_reason = "timeout";
            kill(-child, SIGTERM);
            kill(child, SIGTERM);
            struct timespec grace = {0, 200000000};
            nanosleep(&grace, NULL);

            waited = wait4(child, &status, WNOHANG, &usage);
            if (waited == 0) {
                kill(-child, SIGKILL);
                kill(child, SIGKILL);
                waited = wait4(child, &status, 0, &usage);
            }
        } else if (waited == 0) {
            struct timespec small = {0, 20000000};
            nanosleep(&small, NULL);
        }
    }

    while (stdout_used < sizeof(stdout_buffer) - 1) {
        ssize_t count = read(stdout_pipe[0], stdout_buffer + stdout_used,
                             sizeof(stdout_buffer) - 1 - stdout_used);
        if (count <= 0) break;
        stdout_used += (size_t)count;
    }
    while (stderr_used < sizeof(stderr_buffer) - 1) {
        ssize_t count = read(stderr_pipe[0], stderr_buffer + stderr_used,
                             sizeof(stderr_buffer) - 1 - stderr_used);
        if (count <= 0) break;
        stderr_used += (size_t)count;
    }
    close(stdout_pipe[0]);
    close(stderr_pipe[0]);

    int exit_code = 0;
    int signal_number = 0;
    if (WIFEXITED(status)) exit_code = WEXITSTATUS(status);
    else if (WIFSIGNALED(status)) {
        signal_number = WTERMSIG(status);
        exit_code = 128 + signal_number;
        termination_reason = timed_out ? "timeout" : "signal";
    }

    double elapsed = wall_seconds(&start);
    double cpu = usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1000000.0 +
                 usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1000000.0;
    long peak_kb = usage.ru_maxrss;
#ifdef __linux__
    long peak_bytes = peak_kb * 1024;
#else
    long peak_bytes = peak_kb;
#endif

    printf("{");
    printf("\"job_id\":"); json_escape(stdout, job_id ? job_id : "");
    printf(",\"pid\":%ld,\"exit_code\":%d,\"signal\":%d",
           (long)child, exit_code, signal_number);
    printf(",\"execution_status\":");
    json_escape(stdout, timed_out ? "timed_out" : (exit_code == 0 ? "exited" : "failed"));
    printf(",\"elapsed_seconds\":%.6f,\"cpu_seconds\":%.6f",
           elapsed, cpu);
    printf(",\"peak_memory_bytes\":%ld", peak_bytes);
    printf(",\"termination_reason\":"); json_escape(stdout, termination_reason);
    printf(",\"resource_limit_status\":\"%s\"",
           (memory_mb || cpu_seconds) ? "configured" : "disabled");
    printf(",\"child_stdout\":"); json_escape(stdout, stdout_buffer);
    printf(",\"child_stderr\":"); json_escape(stdout, stderr_buffer);
    printf(",\"platform_metadata\":{\"rlimit_as_bytes\":%llu,\"rlimit_cpu_seconds\":%lu}",
           (unsigned long long)(memory_mb ? memory_mb * 1024ULL * 1024ULL : 0),
           cpu_seconds);
    printf("}\n");

    free(request);
    return timed_out ? 1 : (exit_code == 0 ? 0 : 1);
}
