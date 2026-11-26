#include <pthread.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <jpeglib.h>

#include "encoder_mjpeg.h"

static char errbuf[256];

const char *encoder_mjpeg_get_error() { return errbuf; }

typedef struct {
    int width;
    int height;
    int quality;
    int stride;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    pthread_t thread;
    bool stopping;
    bool data_queued;
    bool is_secondary;
    uint8_t *data_buffer;
    int data_quality;
    uint64_t data_dts;
    uint64_t data_ntp;
    encoder_mjpeg_output_cb output_cb;
} encoder_mjpeg_priv_t;

static void save_as_jpeg(unsigned int width, unsigned int height,
                         unsigned int quality, unsigned int stride,
                         uint8_t *in_buf, uint8_t **out_buf,
                         unsigned long *out_size) {
    struct jpeg_error_mgr jerr;
    struct jpeg_compress_struct cinfo;
    cinfo.err = jpeg_std_error(&jerr);
    jpeg_create_compress(&cinfo);

    cinfo.image_width = width;
    cinfo.image_height = height;
    cinfo.input_components = 3;
    cinfo.in_color_space = JCS_YCbCr;
    jpeg_set_defaults(&cinfo);
    jpeg_set_quality(&cinfo, quality, TRUE);

    jpeg_mem_dest(&cinfo, out_buf, out_size);

    jpeg_start_compress(&cinfo, TRUE);

    JSAMPROW row_pointer[1];
    uint8_t *row_buf = malloc(width * 3);
    row_pointer[0] = row_buf;

    uint8_t *Y = in_buf;
    uint8_t *U = Y + stride * height;
    uint8_t *V = U + (stride / 2) * (height / 2);

    while (cinfo.next_scanline < height) {
        for (unsigned int j = 0; j < width; j++) {
            int i1 = cinfo.next_scanline * stride + j;
            int i2 = cinfo.next_scanline / 2 * stride / 2 + j / 2;

            row_buf[j * 3 + 0] = Y[i1];
            row_buf[j * 3 + 1] = U[i2];
            row_buf[j * 3 + 2] = V[i2];
        }

        jpeg_write_scanlines(&cinfo, row_pointer, 1);
    }

    free(row_buf);

    jpeg_finish_compress(&cinfo);
    jpeg_destroy_compress(&cinfo);
}

static void *thread_main(void *userdata) {
    encoder_mjpeg_priv_t *encp = (encoder_mjpeg_priv_t *)userdata;

    while (true) {
        pthread_mutex_lock(&encp->mutex);

        while (!encp->data_queued && !encp->stopping) {
            pthread_cond_wait(&encp->cond, &encp->mutex);
        }

        if (!encp->data_queued) {
            // Stopping with no queued frame left: finish the thread.
            pthread_mutex_unlock(&encp->mutex);
            break;
        }

        uint8_t *buffer = encp->data_buffer;
        int quality = encp->data_quality;
        uint64_t dts = encp->data_dts;
        uint64_t ntp = encp->data_ntp;
        encp->data_queued = false;

        // Unblock a possible producer; the copied job values keep this frame
        // on its own parameter generation even if reload runs meanwhile.
        pthread_cond_signal(&encp->cond);

        pthread_mutex_unlock(&encp->mutex);

        uint8_t *out_buf = NULL;
        unsigned long out_size = 0;
        save_as_jpeg(encp->width, encp->height, quality, encp->stride, buffer,
                     &out_buf, &out_size);

        if (out_buf != NULL) {
            encp->output_cb(out_buf, out_size, dts, ntp);
            free(out_buf);
        }
    }

    return NULL;
}

bool encoder_mjpeg_create(bool is_secondary, const parameters_t *params,
                          int stride, encoder_mjpeg_output_cb output_cb,
                          encoder_mjpeg_t **enc) {
    *enc = malloc(sizeof(encoder_mjpeg_priv_t));
    encoder_mjpeg_priv_t *encp = (encoder_mjpeg_priv_t *)(*enc);
    memset(encp, 0, sizeof(encoder_mjpeg_priv_t));

    encp->width = (!is_secondary) ? params->width : params->secondary_width;
    encp->height = (!is_secondary) ? params->height : params->secondary_height;
    encp->quality = (!is_secondary) ? params->mjpeg_quality
                                    : params->secondary_mjpeg_quality;
    encp->stride = stride;
    encp->output_cb = output_cb;
    encp->is_secondary = is_secondary;
    pthread_mutex_init(&encp->mutex, NULL);
    pthread_cond_init(&encp->cond, NULL);
    pthread_create(&encp->thread, NULL, thread_main, encp);

    return true;
}

void encoder_mjpeg_encode(encoder_mjpeg_t *enc, uint8_t *buffer_mapped,
                          int buffer_fd, uint64_t dts, uint64_t ntp) {
    encoder_mjpeg_priv_t *encp = (encoder_mjpeg_priv_t *)enc;

    pthread_mutex_lock(&encp->mutex);

    while (encp->data_queued && !encp->stopping) {
        pthread_cond_wait(&encp->cond, &encp->mutex);
    }

    if (encp->stopping) {
        pthread_mutex_unlock(&encp->mutex);
        return;
    }

    encp->data_queued = true;
    encp->data_buffer = buffer_mapped;
    encp->data_quality = encp->quality;
    encp->data_dts = dts;
    encp->data_ntp = ntp;

    pthread_cond_signal(&encp->cond);

    pthread_mutex_unlock(&encp->mutex);
}

void encoder_mjpeg_reload_params(encoder_mjpeg_t *enc,
                                 const parameters_t *params) {
    encoder_mjpeg_priv_t *encp = (encoder_mjpeg_priv_t *)enc;

    pthread_mutex_lock(&encp->mutex);
    encp->quality = (!encp->is_secondary) ? params->mjpeg_quality
                                          : params->secondary_mjpeg_quality;
    pthread_mutex_unlock(&encp->mutex);
}

void encoder_mjpeg_destroy(encoder_mjpeg_t *enc) {
    if (enc == NULL) {
        return;
    }

    encoder_mjpeg_priv_t *encp = (encoder_mjpeg_priv_t *)enc;

    pthread_mutex_lock(&encp->mutex);
    encp->stopping = true;
    pthread_cond_broadcast(&encp->cond);
    pthread_mutex_unlock(&encp->mutex);

    // Joining lets a queued in-flight frame finish before the structure goes
    // away; its output callback is silenced at the pipe boundary by main.
    pthread_join(encp->thread, NULL);

    pthread_cond_destroy(&encp->cond);
    pthread_mutex_destroy(&encp->mutex);
    free(encp);
}
