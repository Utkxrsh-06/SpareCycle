#ifndef UNICODE
#define UNICODE 1
#endif
#ifndef _UNICODE
#define _UNICODE 1
#endif

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <windows.h>
#include <psapi.h>

static const char *argument(int argc, char **argv, const char *name) {
    size_t length = strlen(name);
    for (int i = 1; i < argc - 1; ++i) {
        if (strncmp(argv[i], name, length) == 0 && argv[i][length] == '=')
            return argv[i] + length + 1;
    }
    return NULL;
}

static char *read_stdin(size_t *length) {
    size_t capacity = 4096, used = 0;
    char *buffer = (char *)malloc(capacity);
    if (!buffer) return NULL;
    for (;;) {
        if (used == capacity) {
            capacity *= 2;
            char *grown = (char *)realloc(buffer, capacity);
            if (!grown) { free(buffer); return NULL; }
            buffer = grown;
        }
        DWORD count = 0;
        if (!ReadFile(GetStdHandle(STD_INPUT_HANDLE), buffer + used,
                      (DWORD)(capacity - used), &count, NULL))
            break;
        if (count == 0) break;
        used += count;
    }
    buffer[used < capacity ? used : capacity - 1] = '\0';
    *length = used;
    return buffer;
}

static void append_quoted(char *command, size_t capacity, size_t *used,
                          const char *value) {
    if (*used >= capacity) return;
    size_t remaining = capacity - *used;
    int count = snprintf(command + *used, remaining, "\"%s\"", value);
    if (count > 0) *used += (size_t)count;
}

static void json_escape(const char *value) {
    putchar('"');
    for (const unsigned char *p = (const unsigned char *)value; *p; ++p) {
        switch (*p) {
            case '\\': fputs("\\\\", stdout); break;
            case '"': fputs("\\\"", stdout); break;
            case '\n': fputs("\\n", stdout); break;
            case '\r': fputs("\\r", stdout); break;
            case '\t': fputs("\\t", stdout); break;
            default:
                if (*p < 0x20) printf("\\u%04x", *p);
                else putchar(*p);
        }
    }
    putchar('"');
}

static void read_available(HANDLE pipe, char *buffer, size_t capacity,
                          size_t *used) {
    DWORD available = 0;
    if (!PeekNamedPipe(pipe, NULL, 0, NULL, &available, NULL) || available == 0)
        return;
    if (*used >= capacity - 1) return;
    DWORD count = available;
    if (count > (DWORD)(capacity - 1 - *used)) count = (DWORD)(capacity - 1 - *used);
    DWORD read = 0;
    if (ReadFile(pipe, buffer + *used, count, &read, NULL)) *used += read;
    buffer[*used] = '\0';
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

    unsigned long timeout_ms = (unsigned long)(strtod(timeout_text, NULL) * 1000.0);
    unsigned long memory_mb = memory_text ? strtoul(memory_text, NULL, 10) : 0;
    unsigned long cpu_seconds = cpu_text ? strtoul(cpu_text, NULL, 10) : 0;
    size_t request_length = 0;
    char *request = read_stdin(&request_length);
    if (!request) {
        fprintf(stderr, "could not read execution request\n");
        return 2;
    }

    SECURITY_ATTRIBUTES security = {sizeof(security), NULL, TRUE};
    HANDLE stdin_read = NULL, stdin_write = NULL;
    HANDLE stdout_read = NULL, stdout_write = NULL;
    HANDLE stderr_read = NULL, stderr_write = NULL;
    if (!CreatePipe(&stdin_read, &stdin_write, &security, 0) ||
        !CreatePipe(&stdout_read, &stdout_write, &security, 0) ||
        !CreatePipe(&stderr_read, &stderr_write, &security, 0)) {
        fprintf(stderr, "could not create process pipes: %lu\n", GetLastError());
        free(request);
        return 2;
    }
    SetHandleInformation(stdin_write, HANDLE_FLAG_INHERIT, 0);
    SetHandleInformation(stdout_read, HANDLE_FLAG_INHERIT, 0);
    SetHandleInformation(stderr_read, HANDLE_FLAG_INHERIT, 0);

    char command_line[32768];
    size_t used = 0;
    append_quoted(command_line, sizeof(command_line), &used, python_path);
    if (used + 4 < sizeof(command_line)) { memcpy(command_line + used, " -u ", 4); used += 4; }
    append_quoted(command_line, sizeof(command_line), &used, handler_path);
    command_line[used < sizeof(command_line) ? used : sizeof(command_line) - 1] = '\0';

    STARTUPINFOA startup;
    PROCESS_INFORMATION process;
    ZeroMemory(&startup, sizeof(startup));
    startup.cb = sizeof(startup);
    startup.dwFlags = STARTF_USESTDHANDLES;
    startup.hStdInput = stdin_read;
    startup.hStdOutput = stdout_write;
    startup.hStdError = stderr_write;
    ZeroMemory(&process, sizeof(process));

    ULONGLONG started = GetTickCount64();
    if (!CreateProcessA(NULL, command_line, NULL, NULL, TRUE,
                        CREATE_NEW_PROCESS_GROUP, NULL, working_directory,
                        &startup, &process)) {
        fprintf(stderr, "CreateProcess failed: %lu\n", GetLastError());
        CloseHandle(stdin_read); CloseHandle(stdin_write);
        CloseHandle(stdout_read); CloseHandle(stdout_write);
        CloseHandle(stderr_read); CloseHandle(stderr_write);
        free(request);
        return 2;
    }

    CloseHandle(stdin_read);
    CloseHandle(stdout_write);
    CloseHandle(stderr_write);
    DWORD written = 0;
    WriteFile(stdin_write, request, (DWORD)request_length, &written, NULL);
    CloseHandle(stdin_write);

    HANDLE job = CreateJobObjectA(NULL, NULL);
    if (job) {
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits;
        ZeroMemory(&limits, sizeof(limits));
        limits.BasicLimitInformation.LimitFlags = 0;
        if (memory_mb > 0) {
            limits.BasicLimitInformation.LimitFlags |=
                JOB_OBJECT_LIMIT_PROCESS_MEMORY;
            limits.ProcessMemoryLimit = (SIZE_T)memory_mb * 1024ULL * 1024ULL;
        }
        if (cpu_seconds > 0) {
            limits.BasicLimitInformation.LimitFlags |=
                JOB_OBJECT_LIMIT_PROCESS_TIME;
            limits.BasicLimitInformation.PerProcessUserTimeLimit.QuadPart =
                (LONGLONG)cpu_seconds * 10000000LL;
        }
        limits.BasicLimitInformation.LimitFlags |=
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        if (limits.BasicLimitInformation.LimitFlags &&
            !SetInformationJobObject(job, JobObjectExtendedLimitInformation,
                                     &limits, sizeof(limits)))
            limits.BasicLimitInformation.LimitFlags = 0;
        if (!AssignProcessToJobObject(job, process.hProcess))
            CloseHandle(job), job = NULL;
    }

    char stdout_buffer[65536] = {0};
    char stderr_buffer[65536] = {0};
    size_t stdout_used = 0, stderr_used = 0;
    int timed_out = 0;
    const char *termination_reason = "exit";
    DWORD wait_result = WaitForSingleObject(process.hProcess, timeout_ms);

    if (wait_result == WAIT_TIMEOUT) {
        timed_out = 1;
        termination_reason = "timeout";
        TerminateProcess(process.hProcess, 1);
    }

    WaitForSingleObject(process.hProcess, 5000);
    read_available(stdout_read, stdout_buffer, sizeof(stdout_buffer), &stdout_used);
    read_available(stderr_read, stderr_buffer, sizeof(stderr_buffer), &stderr_used);
    WaitForSingleObject(process.hProcess, INFINITE);
    read_available(stdout_read, stdout_buffer, sizeof(stdout_buffer), &stdout_used);
    read_available(stderr_read, stderr_buffer, sizeof(stderr_buffer), &stderr_used);

    DWORD exit_code = 0;
    GetExitCodeProcess(process.hProcess, &exit_code);

    ULONGLONG elapsed_ms = GetTickCount64() - started;
    FILETIME creation, exit, kernel, user;
    ULONGLONG cpu_seconds_used = 0;
    if (GetProcessTimes(process.hProcess, &creation, &exit, &kernel, &user)) {
        ULONGLONG kernel_100ns =
            ((ULONGLONG)kernel.dwHighDateTime << 32) | kernel.dwLowDateTime;
        ULONGLONG user_100ns =
            ((ULONGLONG)user.dwHighDateTime << 32) | user.dwLowDateTime;
        cpu_seconds_used = (kernel_100ns + user_100ns) / 10000000ULL;
    }

    PROCESS_MEMORY_COUNTERS memory;
    ZeroMemory(&memory, sizeof(memory));
    SIZE_T peak_bytes = 0;
    if (GetProcessMemoryInfo(process.hProcess, &memory, sizeof(memory)))
        peak_bytes = memory.PeakWorkingSetSize;

    DWORD signal_number = timed_out ? 1 : 0;
    printf("{");
    printf("\"job_id\":"); json_escape(job_id ? job_id : "");
    printf(",\"pid\":%lu,\"exit_code\":%lu,\"signal\":%lu",
           (unsigned long)process.dwProcessId, (unsigned long)exit_code,
           (unsigned long)signal_number);
    printf(",\"execution_status\":");
    json_escape(timed_out ? "timed_out" : (exit_code == 0 ? "exited" : "failed"));
    printf(",\"elapsed_seconds\":%.6f", elapsed_ms / 1000.0);
    printf(",\"cpu_seconds\":%.6f",
           cpu_seconds_used + ((cpu_seconds_used == 0) ? 0.0 : 0.000001));
    printf(",\"peak_memory_bytes\":%llu",
           (unsigned long long)peak_bytes);
    printf(",\"termination_reason\":"); json_escape(termination_reason);
    printf(",\"resource_limit_status\":\"%s\"",
           (memory_mb || cpu_seconds) ? "configured" : "disabled");
    printf(",\"child_stdout\":"); json_escape(stdout_buffer);
    printf(",\"child_stderr\":"); json_escape(stderr_buffer);
    printf(",\"platform_metadata\":{\"job_object\":%s}",
           job ? "true" : "false");
    printf("}\n");

    if (job) {
        TerminateJobObject(job, 1);
        CloseHandle(job);
    }
    CloseHandle(process.hProcess);
    CloseHandle(process.hThread);
    CloseHandle(stdout_read);
    CloseHandle(stderr_read);
    free(request);
    return timed_out ? 1 : (exit_code == 0 ? 0 : 1);
}
