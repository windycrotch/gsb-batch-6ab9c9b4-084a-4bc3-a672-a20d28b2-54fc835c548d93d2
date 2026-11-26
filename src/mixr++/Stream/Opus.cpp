#include "mixr++/Stream/Opus.h"

#include <cassert>
#include <cstring>
#include <iostream>
#include <stdexcept>

#define MX_OPUS_MINIMUM_BUFFER_SIZE 11520

namespace mixr::Stream
{
    Opus::Opus(const std::string& path)
    {
        _file = op_open_file(path.c_str(), nullptr);
        if (!_file)
            throw std::runtime_error("Failed to open file.");

        const OpusHead* head = op_head(_file, op_current_link(_file));
        _format = { DataType::F32, head->input_sample_rate, static_cast<uint8_t>(head->channel_count) };
    }

    Opus::~Opus()
    {
        op_free(_file);
    }

    size_t Opus::GetBuffer(uint8_t* buffer, size_t bufferLength)
    {
        std::cout << "Read" << std::endl;
        float* floatBuf = reinterpret_cast<float*>(buffer);
        size_t floatBufSize = bufferLength / 4;

        float buf[MX_OPUS_MINIMUM_BUFFER_SIZE];

        size_t totalRead = 0;
        while (totalRead < floatBufSize)
        {
            size_t amountToRead = std::min(floatBufSize - totalRead, static_cast<size_t>(MX_OPUS_MINIMUM_BUFFER_SIZE));

            int read = op_read_float_stereo(_file, buf, amountToRead);
            if (read == 0)
                return 0;

            if (read < 0)
                throw std::runtime_error("Failed to read. Code " + std::to_string(read));

            memcpy(floatBuf + totalRead, buf, read * 2 * 4);
            totalRead += read * 2;
        }

        assert(totalRead == floatBufSize);
        return totalRead * 4;
    }

    void Opus::Restart()
    {
        int ret = op_pcm_seek(_file, 0);
        if (ret < 0)
            throw std::runtime_error("Failed to seek. Code " + std::to_string(ret));
    }

    void Opus::SeekToSample(size_t sample)
    {
        int ret = op_pcm_seek(_file, static_cast<ogg_int64_t>(sample));
        if (ret < 0)
            throw std::runtime_error("Failed to seek. Code " + std::to_string(ret));
    }

    size_t Opus::PositionInSamples()
    {
        return op_pcm_tell(_file);
    }

    size_t Opus::LengthInSamples()
    {
        return op_pcm_total(_file, -1);
    }

    std::vector<uint8_t> Opus::GetPCM()
    {
        std::vector<uint8_t> buffer(LengthInSamples());
        GetBuffer(buffer.data(), buffer.size());
        return buffer;
    }
}
