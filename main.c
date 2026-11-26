#include <pthread.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>

#include <linux/dma-buf.h>

#include "camera.h"
#include "encoder.h"
#include "parameters.h"
#include "pipe.h"
#include "text.h"

static int pipe_out_fd = -1;

/*
 * Guards every write to pipe_out_fd together with pipe_output_closed.
 * Once shutdown starts, pipe_output_closed is set and every late camera or
 * encoder callback drops its output instead of writing to the pipe.
 */
static pthread_mutex_t pipe_out_mutex = PTHREAD_MUTEX_INITIALIZER;
static bool pipe_output_closed = false;

/*
 * Lifecycle / parameter-generation lock.
 *
 * A whole captured frame (text drawing + main encode + secondary encode) runs
 * while holding this mutex. Parameter reloads take the same mutex for their
 * complete apply phase, which gives:
 *
 *  - atomic updates: a frame can never observe a half-applied command, and the
 *    main and secondary encoders of the same frame always get the same
 *    parameter generation;
 *  - exit convergence: setting shutting_down under this mutex is also a
 *    barrier - the lock call waits for any frame currently touching the text
 *    or encoders to finish, and every later frame callback sees shutting_down
 *    and returns without touching any resource.
 */
static pthread_mutex_t lifecycle_mutex = PTHREAD_MUTEX_INITIALIZER;
static bool shutting_down = false;

static parameters_t *params = NULL;
static camera_t *cam = NULL;
static text_t *text = NULL;
static encoder_t *enc = NULL;
static encoder_t *enc_secondary = NULL;

static void on_frame(uint8_t *buffer_mapped, int buffer_fd, uint64_t dts,
                     uint64_t ntp, uint8_t *secondary_buffer_mapped,
                     int secondary_buffer_fd) {
    pthread_mutex_lock(&lifecycle_mutex);

    /* Exit has started: do not touch text or encoders that may go away. */
    if (shutting_down) {
        pthread_mutex_unlock(&lifecycle_mutex);
        return;
    }

    // mapped DMA buffers require a DMA_BUF_IOCTL_SYNC before and after usage.
    // https://forums.raspberrypi.com/viewtopic.php?t=352554
    // The fd guard also allows non-dmabuf/test buffers.
    if (buffer_fd >= 0) {
        struct dma_buf_sync dma_sync = {0};
        dma_sync.flags = DMA_BUF_SYNC_START | DMA_BUF_SYNC_RW;
        ioctl(buffer_fd, DMA_BUF_IOCTL_SYNC, &dma_sync);

        text_draw(text, buffer_mapped, ntp);

        dma_sync.flags = DMA_BUF_SYNC_END | DMA_BUF_SYNC_RW;
        ioctl(buffer_fd, DMA_BUF_IOCTL_SYNC, &dma_sync);
    } else {
        text_draw(text, buffer_mapped, ntp);
    }

    encoder_encode(enc, buffer_mapped, buffer_fd, dts, ntp);

    if (enc_secondary != NULL && secondary_buffer_mapped != NULL) {
        encoder_encode(enc_secondary, secondary_buffer_mapped,
                       secondary_buffer_fd, dts, ntp);
    }

    pthread_mutex_unlock(&lifecycle_mutex);
}

static void on_encoder_output(const uint8_t *buffer, uint64_t size,
                              uint64_t dts, uint64_t ntp) {
    pthread_mutex_lock(&pipe_out_mutex);
    if (!pipe_output_closed) {
        pipe_write_data(pipe_out_fd, buffer, size, dts, ntp);
    }
    pthread_mutex_unlock(&pipe_out_mutex);
}

static void on_encoder_secondary_output(const uint8_t *buffer, uint64_t size,
                                        uint64_t dts, uint64_t ntp) {
    pthread_mutex_lock(&pipe_out_mutex);
    if (!pipe_output_closed) {
        pipe_write_secondary_data(pipe_out_fd, buffer, size, dts, ntp);
    }
    pthread_mutex_unlock(&pipe_out_mutex);
}

static void on_error() {
    pthread_mutex_lock(&pipe_out_mutex);
    if (!pipe_output_closed) {
        pipe_write_error(pipe_out_fd, "camera driver exited");
    }
    pthread_mutex_unlock(&pipe_out_mutex);
}

/*
 * Applies a fully parsed and validated parameter set to every module.
 * Callers must hold lifecycle_mutex. All modules are updated from the same
 * parameters_t generation, and no frame can interleave with these calls.
 */
static void apply_parameters(parameters_t *new_params) {
    camera_reload_params(cam, new_params);
    text_reload_params(text, new_params);
    encoder_reload_params(enc, new_params);
    if (enc_secondary != NULL) {
        encoder_reload_params(enc_secondary, new_params);
    }

    parameters_t *old_params = params;
    params = new_params;

    /* The lock is released by the caller; old params are no longer referenced
     * by any reload path. Encoder worker threads only keep values copied under
     * their own locks or are joined during destroy. */
    if (old_params != NULL) {
        parameters_destroy(old_params);
    }
}

static bool handle_command(const uint8_t *buf, uint32_t size) {
    if (size < 1) {
        return true;
    }

    switch (buf[0]) {
    case 'e':
        return false;

    case 'c': {
        if (size < 2) {
            printf("skipping reloading parameters since they are invalid: "
                   "empty command\n");
            fflush(stdout);
            break;
        }

        parameters_t *new_params = NULL;
        bool ok = parameters_unserialize(&buf[1], size - 1, &new_params);
        if (!ok) {
            // The candidate was parsed into a standalone object and already
            // discarded by parameters_unserialize(): the live state is
            // untouched, so the previous generation keeps running.
            printf("skipping reloading parameters since they are invalid: %s\n",
                   parameters_get_error());
            fflush(stdout);
            break;
        }

        pthread_mutex_lock(&lifecycle_mutex);
        if (shutting_down) {
            // Exit command was already processed: reject late updates.
            pthread_mutex_unlock(&lifecycle_mutex);
            parameters_destroy(new_params);
            break;
        }
        apply_parameters(new_params);
        pthread_mutex_unlock(&lifecycle_mutex);
        break;
    }
    }

    return true;
}

/*
 * Makes every output callback a no-op from now on. The mutex guarantees that
 * a write already in progress finishes as one complete frame before the flag
 * is observed; no write can start afterwards.
 */
static void close_pipe_output(void) {
    pthread_mutex_lock(&pipe_out_mutex);
    pipe_output_closed = true;
    pthread_mutex_unlock(&pipe_out_mutex);
}

/*
 * Marks the lifecycle as shutting down and waits for every frame callback
 * section that touches the text or encoders to finish.
 */
static void begin_shutdown(void) {
    pthread_mutex_lock(&lifecycle_mutex);
    shutting_down = true;
    pthread_mutex_unlock(&lifecycle_mutex);
}

int main() {
    if (getenv("TEST") != NULL) {
        printf("test passed\n");
        return 0;
    }

    int pipe_in_fd = atoi(getenv("PIPE_CONF_FD"));
    pipe_out_fd = atoi(getenv("PIPE_VIDEO_FD"));

    uint8_t *buf = NULL;
    uint32_t n = pipe_read(pipe_in_fd, &buf);
    if (n == 0) {
        // Configuration pipe reached EOF before the initial configuration.
        return 0;
    }

    bool ok = parameters_unserialize(&buf[1], n - 1, &params);
    free(buf);
    buf = NULL;
    if (!ok) {
        pipe_write_error(pipe_out_fd, "parameters_unserialize(): %s",
                         parameters_get_error());
        return -1;
    }

    // Keep the output lock held during setup so "ready" is the first message
    // ever written, exactly as in the original startup sequence.
    pthread_mutex_lock(&pipe_out_mutex);

    ok = camera_create(params, on_frame, on_error, &cam);
    if (!ok) {
        pipe_write_error(pipe_out_fd, "camera_create(): %s",
                         camera_get_error());
        goto startup_failed;
    }

    ok = text_create(params, camera_get_stride(cam), &text);
    if (!ok) {
        pipe_write_error(pipe_out_fd, "text_create(): %s", text_get_error());
        goto startup_failed;
    }

    ok = encoder_create(false, params, camera_get_frame_size(cam),
                        camera_get_stride(cam), camera_get_colorspace(cam),
                        on_encoder_output, &enc);
    if (!ok) {
        pipe_write_error(pipe_out_fd, "encoder_create(): %s",
                         encoder_get_error());
        goto startup_failed;
    }

    if (params->secondary_width != 0) {
        ok = encoder_create(true, params, camera_get_secondary_frame_size(cam),
                            camera_get_secondary_stride(cam),
                            camera_get_secondary_colorspace(cam),
                            on_encoder_secondary_output, &enc_secondary);
        if (!ok) {
            pipe_write_error(pipe_out_fd, "encoder_create(): %s",
                             encoder_get_error());
            goto startup_failed;
        }
    }

    ok = camera_start(cam, params);
    if (!ok) {
        pipe_write_error(pipe_out_fd, "camera_start(): %s", camera_get_error());
        goto startup_failed;
    }

    pipe_write_ready(pipe_out_fd);
    pthread_mutex_unlock(&pipe_out_mutex);

    while (true) {
        buf = NULL;
        uint32_t size = pipe_read(pipe_in_fd, &buf);

        if (size == 0) {
            // Writer closed the command pipe: converge just like on 'e'.
            break;
        }

        bool keep_running = handle_command(buf, size);
        free(buf);
        buf = NULL;

        if (!keep_running) {
            break;
        }
    }

    /*
     * Observable convergence sequence:
     *
     * 1. stop accepting pipe output immediately (late encoder/camera
     *    callbacks can no longer write video or error messages);
     * 2. reject parameter updates and wait for in-flight frame sections;
     * 3. stop camera acquisition;
     * 4. release resources in the required order:
     *    main encoder -> secondary encoder -> text overlay -> camera.
     *    Each encoder_destroy joins its background/output thread, so queued
     *    in-flight work is finished (with output discarded) before free.
     */
    close_pipe_output();
    begin_shutdown();

    camera_stop(cam);

    // Final exclusion barrier after camera_stop(): any callback that can still
    // be delivered observes shutting_down and returns before touching
    // resources. Acquiring the lock here waits for the one currently inside.
    pthread_mutex_lock(&lifecycle_mutex);
    pthread_mutex_unlock(&lifecycle_mutex);

    encoder_destroy(enc);
    enc = NULL;

    if (enc_secondary != NULL) {
        encoder_destroy(enc_secondary);
        enc_secondary = NULL;
    }

    text_destroy(text);
    text = NULL;

    camera_destroy(cam);
    cam = NULL;

    parameters_destroy(params);
    params = NULL;

    return 0;

startup_failed:
    pipe_output_closed = true;
    pthread_mutex_unlock(&pipe_out_mutex);

    // Only objects that were created are non-NULL. Release in safe reverse
    // dependency order; the camera was not started on these paths.
    if (enc_secondary != NULL) {
        encoder_destroy(enc_secondary);
    }
    if (enc != NULL) {
        encoder_destroy(enc);
    }
    if (text != NULL) {
        text_destroy(text);
    }
    if (cam != NULL) {
        camera_destroy(cam);
    }
    if (params != NULL) {
        parameters_destroy(params);
    }

    return -1;
}
