#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pipe.h"

/* Writes the whole buffer, retrying on short writes and EINTR. A frame is
 * therefore either written completely or not at all from the caller's point
 * of view (callers additionally serialize calls with a mutex). */
static bool write_full(int fd, const void *data, size_t size) {
    const uint8_t *ptr = data;

    while (size > 0) {
        ssize_t n = write(fd, ptr, size);
        if (n > 0) {
            ptr += n;
            size -= (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }

    return true;
}

/* Reads exactly size bytes unless EOF or an error occurs. Returns false when
 * no bytes (or only a partial record) could be read. */
static bool read_full(int fd, void *data, size_t size) {
    uint8_t *ptr = data;

    while (size > 0) {
        ssize_t n = read(fd, ptr, size);
        if (n > 0) {
            ptr += n;
            size -= (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }

    return true;
}

void pipe_write_error(int fd, const char *format, ...) {
    char buf[256];
    buf[0] = 'e';
    va_list args;
    va_start(args, format);
    vsnprintf(&buf[1], sizeof(buf) - 1, format, args);
    buf[sizeof(buf) - 1] = 0x00;
    uint32_t n = (uint32_t)strlen(buf);
    write_full(fd, &n, 4);
    write_full(fd, buf, n);
    va_end(args);
}

void pipe_write_ready(int fd) {
    char buf[] = {'r'};
    uint32_t n = 1;
    write_full(fd, &n, 4);
    write_full(fd, buf, n);
}

void pipe_write_data(int fd, const uint8_t *mapped, uint32_t size, uint64_t dts,
                     uint64_t ntp) {
    char head[] = {'d'};
    uint32_t payload_size = size;
    uint32_t record_size = 1 + (uint32_t)sizeof(uint64_t) +
                           (uint32_t)sizeof(uint64_t) + payload_size;
    write_full(fd, &record_size, 4);
    write_full(fd, head, 1);
    write_full(fd, &dts, sizeof(uint64_t));
    write_full(fd, &ntp, sizeof(uint64_t));
    write_full(fd, mapped, payload_size);
}

void pipe_write_secondary_data(int fd, const uint8_t *mapped, uint32_t size,
                               uint64_t dts, uint64_t ntp) {
    char head[] = {'s'};
    uint32_t payload_size = size;
    uint32_t record_size = 1 + (uint32_t)sizeof(uint64_t) +
                           (uint32_t)sizeof(uint64_t) + payload_size;
    write_full(fd, &record_size, 4);
    write_full(fd, head, 1);
    write_full(fd, &dts, sizeof(uint64_t));
    write_full(fd, &ntp, sizeof(uint64_t));
    write_full(fd, mapped, payload_size);
}

uint32_t pipe_read(int fd, uint8_t **pbuf) {
    *pbuf = NULL;

    uint32_t n;
    if (!read_full(fd, &n, sizeof(n))) {
        return 0;
    }

    /* Guard against malformed/huge lengths before allocating. */
    if (n == 0 || n > (16u * 1024u * 1024u)) {
        return 0;
    }

    uint8_t *buf = malloc(n);
    if (buf == NULL) {
        return 0;
    }

    if (!read_full(fd, buf, n)) {
        free(buf);
        return 0;
    }

    *pbuf = buf;
    return n;
}
