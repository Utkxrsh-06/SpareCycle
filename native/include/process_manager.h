#ifndef SPARECYCLE_PROCESS_MANAGER_H
#define SPARECYCLE_PROCESS_MANAGER_H

#include <stddef.h>

typedef struct {
    char *python_path;
    char *handler_path;
    char *working_directory;
    char *job_id;
    double timeout_seconds;
    unsigned long memory_limit_mb;
    unsigned long cpu_limit_seconds;
} NativeRequest;

#endif
