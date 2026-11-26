#pragma once

#include "AudioStream.h"

#include <opusfile.h>

#include <string>
#include <cstdio>

namespace mixr::Stream
{
    class Opus final : public AudioStream
    {
        OggOpusFile* _file;
        AudioFormat _format;

    public:
        explicit Opus(const std::string& path);
        ~Opus() override;

        AudioFormat Format() override { return _format; }
        size_t GetBuffer(uint8_t* buffer, size_t bufferLength) override;
        void Restart() override;
        void SeekToSample(size_t sample) override;
        size_t PositionInSamples() override;
        size_t LengthInSamples() override;
        std::vector<uint8_t> GetPCM() override;
    };
}