#pragma once

#include "AudioStream.h"
#include "../mixr.h"

#ifdef __cplusplus
extern "C" {
#endif

MIXR_C_API void mxStreamLoadOpus(const char *path, MxAudioStream **stream);

#ifdef __cplusplus
}
#endif