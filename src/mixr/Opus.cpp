#include "mixr/Stream/AudioStream.h"
#include "mixr/Stream/Opus.h"
#include "mixr++/Stream/Opus.h"

using namespace mixr::Stream;

void mxStreamLoadOpus(const char* path, MxAudioStream **pAudioStream) {
    Opus* opus = new Opus(path);
    *pAudioStream = (MxAudioStream*) opus;
}