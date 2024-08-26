#include "log.h"

#include <cstdio>
#include <string>

#include "Enclave_t.h"

void log_log(int level, const char *file, int line, const char *fmt, ...) {
    va_list va;
    va_start(va, fmt);
    int count = vsnprintf(NULL, 0, fmt, va);
    va_end(va);
    std::string buf(count + 1, '\0');
    va_start(va, fmt);
    vsnprintf(&buf[0], buf.size(), fmt, va);
    va_end(va);

    ocall_log(level, file, line, buf.data());
}
