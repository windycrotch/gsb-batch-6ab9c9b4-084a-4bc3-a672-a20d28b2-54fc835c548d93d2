#include <iostream>
#include <string>
#include <algorithm>
#include "Impl.h"

inline float Clamp(float value, float min, float max) {
    return value <= min ? min : value >= max ? max : value;
}

inline float Lerp(float a, float b, float multiplier) {
    return multiplier * (b - a) + a;
}

namespace mixr {
    Impl::Impl(uint32_t sampleRate) {
        _sampleRate = sampleRate;
        _masterVolume = 1.0f;
    }

    size_t Impl::CreateBuffer(uint8_t* data, size_t dataLength) {
        std::lock_guard guard(_mutex);
        Buffer buffer {
           /* .Data = */ std::vector<uint8_t>(data, data + dataLength),
           /* DataLength= */ dataLength,
           /* .Alive = */ true
        };

        size_t index;

        if (_availableBuffers.empty()) {
            index = _buffers.size();
            _buffers.push_back(buffer);
        } else {
            index = _availableBuffers.front();
            _availableBuffers.pop_front();
            _buffers[index] = buffer;
        }

        return index;
    }

    void Impl::DestroyBuffer(size_t bufferId) {
        {
            std::lock_guard guard(_mutex);

            // Defensive: an id that was never handed out, or one whose slot has
            // already been destroyed, is a no-op rather than an out-of-bounds read.
            if (bufferId >= _buffers.size() || !_buffers[bufferId].Alive)
                return;

            Buffer* buffer = &_buffers[bufferId];

            // The buffer may be queued on any number of sources.
            // - If it sits at the front of a source's queue that source is playing
            //   from it, so the source must be stopped.
            // - The buffer is removed from every queue that references it.
            // All of this happens under _mutex, so the mixer thread cannot be
            // mid-read of the buffer while it is being torn down.
            for (size_t i = 0; i < _sources.size(); i++) {
                Source* source = &_sources[i];

                if (!source->Alive)
                    continue;

                auto* queuedBuffers = &source->QueuedBuffers;

                auto loc = std::find(queuedBuffers->begin(), queuedBuffers->end(), bufferId);
                if (loc == queuedBuffers->end())
                    continue;

                if (bufferId == queuedBuffers->front()) {
                    // StopSourceLocked resets position, clears the queue and (if the
                    // state actually changed) enqueues a de-duplicated Stopped event.
                    StopSourceLocked(i);
                } else {
                    // Not at the front: just drop every reference to this buffer.
                    queuedBuffers->erase(std::remove(queuedBuffers->begin(), queuedBuffers->end(), bufferId),
                                         queuedBuffers->end());
                }
            }

            // Drop the payload before parking the slot. Any stale id handed to
            // UpdateBuffer afterwards is rejected by the Alive check there.
            buffer->Data = {};
            buffer->DataLength = 0;
            buffer->Alive = false;

            _availableBuffers.push_back(bufferId);
        }

        // Sources stopped above have now fully settled; deliver their events.
        FlushPendingCallbacks();
    }

    size_t Impl::CreateSource(const SourceDescription& description) {
        std::lock_guard guard(_mutex);
        AudioFormat format = description.Format;

        uint8_t byteAlign = 0;
        switch (format.DataType) {
            case DataType::I8:
            case DataType::U8:
                byteAlign = 1;
                break;
            case DataType::I16:
                byteAlign = 2;
                break;
            case DataType::I32:
            case DataType::F32:
                byteAlign = 4;
                break;
        }

        int channels = format.Channels;

        if (channels < 1 || channels > 2) {
            throw std::runtime_error("Unsupported number of channels: " + std::to_string(channels));
        }

        SourceType type = description.Type;
        auto chunkSize = description.ADPCM.ChunkSize;

        uint8_t* buffer = nullptr;

        switch (type)
        {
            case SourceType::PCM:
                break;
            case SourceType::ADPCM:
                buffer = new uint8_t[(chunkSize - (channels * 4)) * 4];
                break;
        }

        Source source {
           /* .Type = */ type,
           /* .Format = */ format,
           /* .ByteAlign = */ byteAlign,
           /* .StereoAlign = */ byteAlign * (channels - 1),
            // Corrects for the sample rate - if the Mixer sample rate is 48khz, and the buffer sample rate is 44.1khz,
            // then this value will be 0.91xyzw, causing the mixer to play the buffer slightly slower to correct the speed.
           /* .SpeedCorrection = */ static_cast<float>(format.SampleRate) / static_cast<float>(_sampleRate),

           /* .QueuedBuffers = */ {},
           /* .MixBuffer = */ buffer,

           /* .Playing = */ false,
           /* .Speed = */ 1.0,
           /* .MainVolume = */ 1.0f,
           /* .Looping = */ false,

           /* .LengthInSamples = */ 0,

           /* .VolumeL = */ 1.0f,
           /* .VolumeR = */ 1.0f,

           /* .Position = */ 0,
           /* .FinePosition = */ 0.0,

           /* .BufferFinishedCallback = */ nullptr,
           /* .BufferFinishedUserData = */ nullptr,

           /* .StateChangedCallback = */ nullptr,
           /* .StateChangedUserData = */ nullptr,

           /* .LastPosition = */ 0,
           /* .LastSampleL = */ 0.0f,
           /* .LastSampleR = */ 0.0f,
           /* .LastChunk = */ (size_t) -1,

           /* .ChunkSize = */ chunkSize,
           /* .NumChunks = */ 0,

           /* .Generation = */ 0,
           /* .Alive = */ true,
           /* .LastNotifiedState = */ SourceState::Stopped
        };

        size_t index;

        if (_availableSources.empty()) {
            index = _sources.size();
            source.Generation = 1;
            _sources.push_back(source);
        } else {
            index = _availableSources.front();
            _availableSources.pop_front();
            // A recycled slot must not inherit anything from its previous life:
            // bump the generation so any in-flight events for the old source are
            // rejected, and start from a fully initialised object.
            source.Generation = _sources[index].Generation + 1;
            _sources[index] = source;
        }

        return index;
    }

    void Impl::DestroySource(size_t sourceId) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return;

        Source* source = &_sources[sourceId];

        // Detach the callbacks first: a destroy must not notify state changes or
        // buffer completion to the owner that is releasing the source.
        source->StateChangedCallback = nullptr;
        source->BufferFinishedCallback = nullptr;

        StopSourceLocked(sourceId);

        // Any events queued before the callbacks were detached belong to the
        // dying generation; drop them rather than letting them reach a recycled
        // slot.
        RemovePendingEventsLocked(sourceId);

        source->QueuedBuffers = {};
        delete source->MixBuffer;
        source->MixBuffer = nullptr;
        source->Alive = false;

        _availableSources.push_back(sourceId);
    }

    void Impl::UpdateBuffer(size_t bufferId, uint8_t* data, size_t dataLength) {
        std::lock_guard guard(_mutex);

        if (bufferId >= _buffers.size() || !_buffers[bufferId].Alive)
            return;

        Buffer* buffer = &_buffers[bufferId];

        // Resize the buffer if it is not big enough, but otherwise, don't.
        // This may use more memory in some situations, but it prevents unnecessary allocations.
        if (dataLength > buffer->Data.size()) {
            buffer->Data.resize(dataLength);
        }

        std::copy(data, data + dataLength, buffer->Data.data());
        buffer->DataLength = dataLength;
    }

    void Impl::SourceSubmitBuffer(size_t sourceId, size_t bufferId) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return;

        if (bufferId >= _buffers.size() || !_buffers[bufferId].Alive)
            return;

        Source* source = &_sources[sourceId];
        source->QueuedBuffers.push_back(bufferId);

        if (source->QueuedBuffers.size() == 1) {
            UpdateSource(source);
        }
    }

    void Impl::SourceClearBuffers(size_t sourceId) {
        {
            std::lock_guard guard(_mutex);

            if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
                return;

            // Spec: after clearing, the source is stopped and its queue is empty.
            // StopSourceLocked performs the full reset and de-duplicated notification.
            StopSourceLocked(sourceId);
            _sources[sourceId].QueuedBuffers = {};
        }

        FlushPendingCallbacks();
    }

    void Impl::SourcePlay(size_t sourceId) {
        {
            std::lock_guard guard(_mutex);

            if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
                return;

            Source* source = &_sources[sourceId];

            if (source->QueuedBuffers.empty())
                return;

            source->Playing = true;

            // The state has settled (queue non-empty, Playing set); request the
            // notification, which is de-duplicated against the last one sent.
            RequestStateLocked(source, sourceId, SourceState::Playing);
        }

        FlushPendingCallbacks();
    }

    void Impl::SourcePause(size_t sourceId) {
        {
            std::lock_guard guard(_mutex);

            if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
                return;

            Source* source = &_sources[sourceId];

            source->Playing = false;

            // A source with no queued buffers and no position is Stopped, not Paused;
            // only announce Paused when the settled state really is Paused.
            SourceState state = SourceGetStateLocked(source);
            if (state == SourceState::Paused) {
                RequestStateLocked(source, sourceId, SourceState::Paused);
            }
        }

        FlushPendingCallbacks();
    }

    void Impl::SourceStop(size_t sourceId, bool useMutex) {
        if (useMutex) {
            {
                std::lock_guard guard(_mutex);

                if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
                    return;

                StopSourceLocked(sourceId);
            }

            FlushPendingCallbacks();
        } else {
            StopSourceLocked(sourceId);
        }
    }

    void Impl::SourceSetSpeed(size_t sourceId, double speed) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return;

        _sources[sourceId].Speed = speed;
    }

    void Impl::SourceSetVolume(size_t sourceId, float volume) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return;

        _sources[sourceId].MainVolume = volume;
    }

    void Impl::SourceSetLooping(size_t sourceId, bool looping) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return;

        _sources[sourceId].Looping = looping;
    }

    void Impl::SourceSetPanning(size_t sourceId, float panning) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return;

        Source* source = &_sources[sourceId];

        source->VolumeL = Clamp(1 - panning, 0.0f, 1.0f);
        source->VolumeR = Clamp(1 - -panning, 0.0f, 1.0f);
    }

    void Impl::SourceSetChannelVolumes(size_t sourceId, float volumeL, float volumeR) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return;

        Source* source = &_sources[sourceId];

        source->VolumeL = volumeL;
        source->VolumeR = volumeR;
    }

    void Impl::SourceSetBufferFinishedCallback(size_t sourceId, void (*callback)(void*), void* userData) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return;

        Source* source = &_sources[sourceId];

        source->BufferFinishedCallback = callback;
        source->BufferFinishedUserData = userData;
    }

    void Impl::SourceSetStateChangedCallback(size_t sourceId, void (*callback)(SourceState, void*), void* userData) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return;

        Source* source = &_sources[sourceId];

        source->StateChangedCallback = callback;
        source->StateChangedUserData = userData;
    }

    SourceState Impl::SourceGetState(size_t sourceId) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return SourceState::Stopped;

        return SourceGetStateLocked(&_sources[sourceId]);
    }

    double Impl::SourceGetSpeed(size_t sourceId) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return 1.0;

        return _sources[sourceId].Speed;
    }

    float Impl::SourceGetVolume(size_t sourceId) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return 0.0f;

        return _sources[sourceId].MainVolume;
    }

    bool Impl::SourceGetLooping(size_t sourceId) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return false;

        return _sources[sourceId].Looping;
    }

    float Impl::SourceGetPanning(size_t sourceId) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return 0.0f;

        Source* source = &_sources[sourceId];

        // -L + R
        // -1 + 1 = Panning 0
        // -0 + 1 = Panning 1
        // etc
        // The returned value may be "incorrect" if you set the channel volumes separately, but panning != channel volumes,
        // they just affect the same values.
        return Clamp(source->VolumeR + -source->VolumeL, -1, 1);
    }

    void Impl::SourceGetChannelVolumes(size_t sourceId, float* volumeL, float* volumeR) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive) {
            if (volumeL) *volumeL = 0.0f;
            if (volumeR) *volumeR = 0.0f;
            return;
        }

        Source* source = &_sources[sourceId];

        *volumeL = source->VolumeL;
        *volumeR = source->VolumeR;
    }

    size_t Impl::SourceGetPositionSamples(size_t sourceId) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return 0;

        return _sources[sourceId].Position;
    }

    double Impl::SourceGetPositionSeconds(size_t sourceId) {
        std::lock_guard guard(_mutex);

        if (sourceId >= _sources.size() || !_sources[sourceId].Alive)
            return 0.0;

        Source* source = &_sources[sourceId];

        return (double) source->Position / (double) source->Format.SampleRate;
    }

    float Impl::GetMasterVolume() {
        std::lock_guard guard(_mutex);
        return _masterVolume;
    }

    void Impl::SetMasterVolume(float volume) {
        std::lock_guard guard(_mutex);
        _masterVolume = volume;
    }

    SourceState Impl::SourceGetStateLocked(const Source* source) const {
        if (source->Playing) {
            return SourceState::Playing;
        } else if (source->Position == 0 && source->QueuedBuffers.empty()) {
            return SourceState::Stopped;
        } else {
            return SourceState::Paused;
        }
    }

    void Impl::RequestStateLocked(Source* source, size_t sourceId, SourceState state) {
        // De-duplicate: the same state transition is never announced twice in a
        // row. The actual callback runs later, without the lock, after the state
        // has settled.
        if (source->LastNotifiedState == state)
            return;

        source->LastNotifiedState = state;

        if (!source->StateChangedCallback)
            return;

        _pendingStateEvents.push_back(PendingStateEvent {
            /* .SourceId = */ sourceId,
            /* .Generation = */ source->Generation,
            /* .State = */ state,
            /* .Callback = */ source->StateChangedCallback,
            /* .UserData = */ source->StateChangedUserData
        });
    }

    void Impl::StopSourceLocked(size_t sourceId) {
        Source* source = &_sources[sourceId];

        SourceState state = SourceGetStateLocked(source);

        source->Playing = false;
        source->QueuedBuffers.clear();
        source->Position = 0;
        source->LastPosition = 0;
        source->LastSampleL = 0.0f;
        source->LastSampleR = 0.0f;
        source->FinePosition = 0.0;
        source->LastChunk = (size_t) -1;

        // Enqueue the notification only if the settled state actually differs
        // from the last one delivered; this makes repeated/concurrent stops
        // produce at most one Stopped notification.
        if (state != SourceState::Stopped) {
            RequestStateLocked(source, sourceId, SourceState::Stopped);
        }
    }

    void Impl::RemovePendingEventsLocked(size_t sourceId) {
        _pendingStateEvents.erase(
            std::remove_if(_pendingStateEvents.begin(), _pendingStateEvents.end(),
                           [sourceId](const PendingStateEvent& event) {
                               return event.SourceId == sourceId;
                           }),
            _pendingStateEvents.end());

        _pendingBufferFinishedEvents.erase(
            std::remove_if(_pendingBufferFinishedEvents.begin(), _pendingBufferFinishedEvents.end(),
                           [sourceId](const PendingBufferFinishedEvent& event) {
                               return event.SourceId == sourceId;
                           }),
            _pendingBufferFinishedEvents.end());
    }

    void Impl::FlushPendingCallbacks() {
        // Swap the queues out under the lock, then invoke with no lock held.
        // This keeps the mixer state consistent when a callback re-enters the
        // context and prevents a slow callback from blocking the mixing thread.
        std::vector<PendingStateEvent> stateEvents;
        std::vector<PendingBufferFinishedEvent> finishedEvents;

        {
            std::lock_guard guard(_mutex);
            stateEvents.swap(_pendingStateEvents);
            finishedEvents.swap(_pendingBufferFinishedEvents);
        }

        for (const auto& event : stateEvents) {
            // Generation guard: the slot may have been destroyed and recycled
            // between enqueue and delivery. Only deliver if it is still the same
            // incarnation and is still alive.
            if (event.SourceId >= _sources.size())
                continue;

            const Source& source = _sources[event.SourceId];
            if (!source.Alive || source.Generation != event.Generation)
                continue;

            event.Callback(event.State, event.UserData);
        }

        for (const auto& event : finishedEvents) {
            if (event.SourceId >= _sources.size())
                continue;

            const Source& source = _sources[event.SourceId];
            if (!source.Alive || source.Generation != event.Generation)
                continue;

            event.Callback(event.UserData);
        }
    }

    inline float GetSample(const uint8_t* data, size_t index, DataType dataType) {
        switch (dataType) {
            case DataType::I8:
                return (float) (int8_t) data[index] / (float) INT8_MAX;
            case DataType::U8:
                return (float) (int8_t) (data[index] - INT8_MAX) / (float) INT8_MAX;
            case DataType::I16:
                return (float) (int16_t) (data[index + 0] | data[index + 1] << 8) / (float) INT16_MAX;
            case DataType::I32:
                return (float) (data[index + 0] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24)) / (float) INT32_MAX;
            case DataType::F32: {
                uint32_t value = (data[index + 0] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24));
                return *(float*) &value;
            }
        }

        return 0;
    }

    void Impl::MixToStereoF32Buffer(float* buffer, size_t bufferLength) {
        // All inspection and mutation of sources/buffers happens under the lock.
        // A concurrent stop, clear, buffer-destroy or source-destroy therefore
        // either completes entirely before this mix pass or waits until it is
        // done; the mixer can never observe a half-cleared queue or a freed
        // buffer payload. Callbacks produced here are deferred until below.
        {
            std::lock_guard guard(_mutex);
            for (int i = 0; i < (int) bufferLength; i += 2) {
                buffer[i + 0] = 0;
                buffer[i + 1] = 0;

                for (int s = 0; s < (int) _sources.size(); s++) {
                    Source* source = &_sources[s];

                    if (!source->Alive || !source->Playing || source->MainVolume <= 0.01f)
                        continue;

                    // Defensive: a playing source must always have a front buffer.
                    // If invariant damage left the queue empty, stop cleanly
                    // instead of dereferencing an empty deque.
                    if (source->QueuedBuffers.empty()) {
                        StopSourceLocked(s);
                        continue;
                    }

                    // If the source speed (the speed that the mixer actually plays at) is above 1, then aliasing is
                    // introduced. So, when the source speed is above 1, we instead sample at a multiple of the mixer
                    // sample rate. So if the source speed was 1.1, and the mixer was sampling at 48khz, then it would sample
                    // at 96khz instead.
                    // Then, at the end, we simply remove the samples that aren't needed. Doing it in this way removes aliasing.
                    double sourceSpeed = source->SpeedCorrection * source->Speed;
                    int sampleRateMultiplier = std::max((int) sourceSpeed, 1) << 1;
                    sourceSpeed /= sampleRateMultiplier;

                    for (int c = 0; c < sampleRateMultiplier; c++) {
                        // Re-resolve every iteration: advancing to the next
                        // queued buffer changes the front.
                        size_t frontBufferId = source->QueuedBuffers.front();
                        Buffer* buf = &_buffers[frontBufferId];

                        uint8_t* bufferData = buf->Data.data();
                        AudioFormat* format = &source->Format;

                        size_t bytePosition = source->Position * (source->ByteAlign + source->StereoAlign);

                        float sampleL = 0.0f, sampleR = 0.0f;

                        switch (source->Type) {
                            case SourceType::PCM: {
                                sampleL = GetSample(bufferData, bytePosition, format->DataType);
                                sampleR = GetSample(bufferData, bytePosition + source->StereoAlign, format->DataType);

                                break;
                            }

                            case SourceType::ADPCM: {
                                auto stereo = source->Format.Channels == 2;
                                size_t chunkSize = source->ChunkSize;
                                size_t dataSize = (chunkSize - (stereo ? 8 : 4)) * 4;

                                size_t chunk = bytePosition / dataSize;

                                if (chunk < source->NumChunks && chunk != source->LastChunk) {
                                    source->LastChunk = chunk;
                                    Utils::ADPCM::DecodeIMAChunk(buf->Data.data() + (chunk * chunkSize), chunkSize, source->MixBuffer, stereo);
                                }

                                size_t newBytePosition = bytePosition % dataSize;

                                uint8_t* sBuf = source->MixBuffer;

                                sampleL = (float) (int16_t) (sBuf[newBytePosition + 0] | sBuf[newBytePosition + 1] << 8) / (float) INT16_MAX;
                                sampleR = (float) (int16_t) (sBuf[newBytePosition + 2] | sBuf[newBytePosition + 3] << 8) / (float) INT16_MAX;

                                break;
                            }
                        }

                        if (c == 0) {
                            float lastSampleL = source->LastSampleL;
                            float lastSampleR = source->LastSampleR;

                            float outSampleL = Lerp(lastSampleL, sampleL, (float) source->FinePosition) * source->VolumeL * source->MainVolume;
                            float outSampleR = Lerp(lastSampleR, sampleR, (float) source->FinePosition) * source->VolumeR * source->MainVolume;

                            buffer[i + 0] += Clamp(outSampleL, -1.0f, 1.0f);
                            buffer[i + 1] += Clamp(outSampleR, -1.0f, 1.0f);
                        }

                        source->FinePosition += sourceSpeed;

                        int intPos = (int) source->FinePosition;
                        source->Position += intPos;
                        source->FinePosition -= intPos;

                        if (source->Position != source->LastPosition) {
                            source->LastPosition = source->Position;
                            source->LastSampleL = sampleL;
                            source->LastSampleR = sampleR;
                        }

                        if (source->Position >= source->LengthInSamples) {
                            if (source->QueuedBuffers.size() > 1) {
                                // The current buffer finished and another is queued.
                                // Record the buffer-finished event (delivered out
                                // of the lock) and advance the queue.
                                if (source->BufferFinishedCallback) {
                                    _pendingBufferFinishedEvents.push_back(PendingBufferFinishedEvent {
                                        /* .SourceId = */ (size_t) s,
                                        /* .Generation = */ source->Generation,
                                        /* .Callback = */ source->BufferFinishedCallback,
                                        /* .UserData = */ source->BufferFinishedUserData
                                    });
                                }

                                source->QueuedBuffers.pop_front();
                                source->Position = 0;
                                source->LastPosition = 0;
                                source->FinePosition = 0.0;
                                UpdateSource(source);
                            } else if (source->Looping) {
                                source->Position -= source->LengthInSamples;
                            } else {
                                // Last buffer finished: settle into Stopped now,
                                // under the lock, with a de-duplicated event.
                                StopSourceLocked((size_t) s);

                                break;
                            }
                        }
                    }
                }

                buffer[i + 0] = Clamp(buffer[i + 0] * _masterVolume, -1.0f, 1.0f);
                buffer[i + 1] = Clamp(buffer[i + 1] * _masterVolume, -1.0f, 1.0f);
            }
        }

        // Deliver any notifications now that every state transition has settled
        // and the lock is released.
        FlushPendingCallbacks();
    }

    void Impl::UpdateSource(Source* source) {
        auto dataLength = _buffers[source->QueuedBuffers.front()].DataLength;
        auto byteAlign = source->ByteAlign;
        auto channels = source->Format.Channels;

        switch (source->Type) {
            case SourceType::PCM:
                source->LengthInSamples = dataLength / (byteAlign * channels);
                break;

            case SourceType::ADPCM: {
                auto numChunks = dataLength / source->ChunkSize;
                source->NumChunks = numChunks;
                int bytesToRemovePerChunk = channels * 4;
                source->LengthInSamples = ((dataLength * 4) - (bytesToRemovePerChunk * numChunks)) / (byteAlign * channels);
                break;
            }
        }
    }
}
