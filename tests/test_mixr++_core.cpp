// Core concurrency/lifetime tests for mixr++.
//
// These tests drive the mixer implementation directly through the public
// Context/AudioSource/AudioBuffer API with synthetic PCM data - no audio
// device, files or external libraries are required, so the suite can run in
// any local build.
//
// Coverage:
//   1. Stop while the mixing thread is producing frames
//   2. ClearBuffers while playing (source ends stopped, queue empty)
//   3. Destroying a buffer that is at the front of a playing source's queue
//   4. Destroying a buffer queued behind the front buffer
//   5. Destroying a source while the mixer is running + id reuse has no residue
//   6. State-changed de-duplication and "fired after the state settled"
//   7. Buffer-finished callbacks and natural stop semantics
//   8. Concurrent stop / clear / destroy against a running mixer (stress)
//   9. Re-entrant callbacks (callback calls back into the context; must not deadlock)

#include <mixr++/Context.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

using namespace mixr;

namespace {

    int g_failures = 0;
    int g_checks = 0;

    void Check(bool condition, const char* expression, const char* file, int line) {
        g_checks++;
        if (!condition) {
            g_failures++;
            std::cout << "  FAIL: " << expression << " (" << file << ":" << line << ")" << std::endl;
        }
    }

#define CHECK(expr) Check((expr), #expr, __FILE__, __LINE__)

    // Build a mono 16-bit PCM buffer whose samples form a recognisable ramp.
    std::vector<uint8_t> MakePcmS16(size_t sampleCount, int16_t base = 0) {
        std::vector<uint8_t> data(sampleCount * sizeof(int16_t));
        for (size_t i = 0; i < sampleCount; i++) {
            int16_t sample = static_cast<int16_t>(base + static_cast<int>(i % 200) - 100);
            std::memcpy(data.data() + i * sizeof(int16_t), &sample, sizeof(int16_t));
        }
        return data;
    }

    SourceDescription MakePcmDescription(uint32_t sampleRate = 48000, uint8_t channels = 1) {
        SourceDescription description{};
        description.Type = SourceType::PCM;
        description.Format.DataType = DataType::I16;
        description.Format.SampleRate = sampleRate;
        description.Format.Channels = channels;
        return description;
    }

    // Run `frames` stereo frames through the context.
    void MixFrames(Context& context, size_t frames) {
        std::vector<float> out(frames * 2, 0.0f);
        context.MixToStereoF32Buffer(out.data(), out.size());
    }

    struct StateCounters {
        std::atomic<int> Playing{0};
        std::atomic<int> Paused{0};
        std::atomic<int> Stopped{0};
        std::atomic<int> LastStateRaw{-1};
    };

    void StateCallback(SourceState state, void* userData) {
        auto* counters = static_cast<StateCounters*>(userData);
        counters->LastStateRaw = static_cast<int>(state);
        switch (state) {
            case SourceState::Playing: counters->Playing++; break;
            case SourceState::Paused:  counters->Paused++;  break;
            case SourceState::Stopped: counters->Stopped++; break;
        }
    }

    struct FinishCounters {
        std::atomic<int> Finished{0};
    };

    void FinishCallback(void* userData) {
        static_cast<FinishCounters*>(userData)->Finished++;
    }

    // ---------------------------------------------------------------------
    // 1. Stop during playback: state, position and removal from the mix.
    // ---------------------------------------------------------------------
    void TestStopDuringPlayback() {
        std::cout << "[1] Stop during playback" << std::endl;

        Context context(48000);
        auto data = MakePcmS16(4096);
        auto buffer = context.CreateBuffer(data.data(), data.size());
        auto source = context.CreateSource(MakePcmDescription());

        StateCounters counters;
        source->SetStateChangedCallback(StateCallback, &counters);

        source->SubmitBuffer(buffer.get());
        source->Play();
        CHECK(counters.Playing.load() == 1);
        CHECK(source->State() == SourceState::Playing);

        // Advance enough that the position is provably non-zero.
        MixFrames(context, 512);
        CHECK(source->Position() > 0);

        source->Stop();

        CHECK(source->State() == SourceState::Stopped);
        CHECK(source->Position() == 0);
        CHECK(counters.Stopped.load() == 1);

        // A stopped source contributes nothing and must not restart.
        std::vector<float> out(256, 1.0f);
        context.MixToStereoF32Buffer(out.data(), out.size());
        bool silence = true;
        for (float v : out) {
            if (v != 0.0f) { silence = false; break; }
        }
        CHECK(silence);
        CHECK(source->Position() == 0);
        CHECK(source->State() == SourceState::Stopped);

        // Repeated stops must not re-notify.
        source->Stop();
        source->Stop();
        CHECK(counters.Stopped.load() == 1);
    }

    // ---------------------------------------------------------------------
    // 2. ClearBuffers while playing: stopped + empty queue.
    // ---------------------------------------------------------------------
    void TestClearBuffers() {
        std::cout << "[2] ClearBuffers while playing" << std::endl;

        Context context(48000);
        auto data1 = MakePcmS16(2048, 0);
        auto data2 = MakePcmS16(2048, 1000);
        auto buffer1 = context.CreateBuffer(data1.data(), data1.size());
        auto buffer2 = context.CreateBuffer(data2.data(), data2.size());
        auto source = context.CreateSource(MakePcmDescription());

        StateCounters counters;
        source->SetStateChangedCallback(StateCallback, &counters);

        source->SubmitBuffer(buffer1.get());
        source->SubmitBuffer(buffer2.get());
        source->Play();
        MixFrames(context, 256);
        CHECK(source->State() == SourceState::Playing);

        source->ClearBuffers();

        CHECK(source->State() == SourceState::Stopped);
        CHECK(source->Position() == 0);
        CHECK(counters.Stopped.load() == 1);

        // The queue is gone: playing again must be a no-op (nothing to play).
        source->Play();
        CHECK(source->State() == SourceState::Stopped);
        CHECK(counters.Playing.load() == 1); // no second Playing notification

        // Mixing must stay silent and must not dereference the cleared queue.
        MixFrames(context, 4096);
        CHECK(source->State() == SourceState::Stopped);

        // Re-queueing after a clear works from a clean slate.
        source->SubmitBuffer(buffer1.get());
        source->Play();
        CHECK(source->State() == SourceState::Playing);
        CHECK(counters.Playing.load() == 2);
        source->Stop();
    }

    // ---------------------------------------------------------------------
    // 3. Destroy the front buffer of a playing source.
    // ---------------------------------------------------------------------
    void TestDestroyFrontBuffer() {
        std::cout << "[3] Destroy front buffer of a playing source" << std::endl;

        Context context(48000);
        auto data1 = MakePcmS16(2048);
        auto data2 = MakePcmS16(2048, 500);
        auto buffer1 = context.CreateBuffer(data1.data(), data1.size());
        auto buffer2 = context.CreateBuffer(data2.data(), data2.size());
        auto source = context.CreateSource(MakePcmDescription());

        StateCounters counters;
        source->SetStateChangedCallback(StateCallback, &counters);

        source->SubmitBuffer(buffer1.get());
        source->SubmitBuffer(buffer2.get());
        source->Play();
        MixFrames(context, 128);
        CHECK(source->State() == SourceState::Playing);

        // Destroying the buffer the source is reading from stops the source and
        // removes the buffer from every queue.
        buffer1.reset();

        CHECK(source->State() == SourceState::Stopped);
        CHECK(source->Position() == 0);
        CHECK(counters.Stopped.load() == 1);

        // The mixer must keep running safely afterwards (no dangling read).
        MixFrames(context, 4096);
        CHECK(source->State() == SourceState::Stopped);

        // The surviving buffer is still usable on a fresh source.
        auto source2 = context.CreateSource(MakePcmDescription());
        source2->SubmitBuffer(buffer2.get());
        source2->Play();
        MixFrames(context, 64);
        CHECK(source2->State() == SourceState::Playing);
    }

    // ---------------------------------------------------------------------
    // 4. Destroy a non-front (queued) buffer: playback continues, queue shrinks.
    // ---------------------------------------------------------------------
    void TestDestroyQueuedBuffer() {
        std::cout << "[4] Destroy a queued (non-front) buffer" << std::endl;

        Context context(48000);
        auto data1 = MakePcmS16(48000); // one second worth, so it stays front
        auto data2 = MakePcmS16(2048, 900);
        auto buffer1 = context.CreateBuffer(data1.data(), data1.size());
        auto buffer2 = context.CreateBuffer(data2.data(), data2.size());
        auto source = context.CreateSource(MakePcmDescription());

        StateCounters counters;
        FinishCounters finish;
        source->SetStateChangedCallback(StateCallback, &counters);
        source->SetBufferFinishedCallback(FinishCallback, &finish);

        source->SubmitBuffer(buffer1.get());
        source->SubmitBuffer(buffer2.get());
        source->Play();
        MixFrames(context, 64);

        buffer2.reset(); // queued behind buffer1 - must not stop the source

        CHECK(source->State() == SourceState::Playing);
        CHECK(counters.Stopped.load() == 0);

        // Play the front buffer to the end. With the second buffer removed, the
        // source stops naturally instead of advancing into a destroyed buffer.
        MixFrames(context, 48000);
        CHECK(source->State() == SourceState::Stopped);
        CHECK(counters.Stopped.load() == 1);
        CHECK(finish.Finished.load() == 0); // no finished transition ever happened
    }

    // ---------------------------------------------------------------------
    // 5. Source destruction and id reuse: no stale queue/callbacks.
    // ---------------------------------------------------------------------
    void TestSourceDestroyAndReuse() {
        std::cout << "[5] Destroy source and reuse its slot" << std::endl;

        Context context(48000);
        auto data = MakePcmS16(2048);
        auto buffer = context.CreateBuffer(data.data(), data.size());

        size_t firstId;
        StateCounters counters;
        {
            auto source = context.CreateSource(MakePcmDescription());
            firstId = source->ID();
            source->SetStateChangedCallback(StateCallback, &counters);
            source->SubmitBuffer(buffer.get());
            source->Play();
            MixFrames(context, 64);
            CHECK(source->State() == SourceState::Playing);
        } // destructor destroys the source

        // A recycled slot must come back in the initial stopped state.
        auto reused = context.CreateSource(MakePcmDescription());
        CHECK(reused->ID() == firstId);
        CHECK(reused->State() == SourceState::Stopped);
        CHECK(reused->Position() == 0);

        // No old state notifications must leak to the new incarnation.
        int stoppedBefore = counters.Stopped.load();
        reused->Stop();
        reused->ClearBuffers();
        CHECK(counters.Stopped.load() == stoppedBefore);

        // The recycled source works correctly with fresh data.
        StateCounters counters2;
        reused->SetStateChangedCallback(StateCallback, &counters2);
        reused->SubmitBuffer(buffer.get());
        reused->Play();
        CHECK(reused->State() == SourceState::Playing);
        CHECK(counters2.Playing.load() == 1);
        MixFrames(context, 32);
        CHECK(reused->Position() > 0);
    }

    // ---------------------------------------------------------------------
    // 6. State notification de-duplication across entry points.
    // ---------------------------------------------------------------------
    void TestStateNotificationDedup() {
        std::cout << "[6] State notification de-duplication" << std::endl;

        Context context(48000);
        auto data = MakePcmS16(4096);
        auto buffer = context.CreateBuffer(data.data(), data.size());
        auto source = context.CreateSource(MakePcmDescription());

        StateCounters counters;
        source->SetStateChangedCallback(StateCallback, &counters);

        // Stopped -> Stopped must not notify.
        source->Stop();
        CHECK(counters.Stopped.load() == 0);

        source->SubmitBuffer(buffer.get());
        source->Play();
        CHECK(counters.Playing.load() == 1);
        // Duplicate play requests while already playing do not re-notify.
        source->Play();
        source->Play();
        CHECK(counters.Playing.load() == 1);

        // Pause after some playback: position retained, Paused announced once.
        MixFrames(context, 128);
        size_t pausedPosition = source->Position();
        CHECK(pausedPosition > 0);
        source->Pause();
        CHECK(source->State() == SourceState::Paused);
        CHECK(counters.Paused.load() == 1);
        source->Pause();
        CHECK(counters.Paused.load() == 1);
        CHECK(source->Position() == pausedPosition); // pause keeps position

        // Resume announces Playing once more.
        source->Play();
        CHECK(counters.Playing.load() == 2);

        // Natural finish announces Stopped exactly once, then a concurrent
        // explicit Stop must not announce it again.
        MixFrames(context, 8192);
        CHECK(source->State() == SourceState::Stopped);
        CHECK(counters.Stopped.load() == 1);
        source->Stop();
        CHECK(counters.Stopped.load() == 1);
    }

    // ---------------------------------------------------------------------
    // 7. Buffer-finished callback timing and natural stop.
    // ---------------------------------------------------------------------
    void TestBufferFinishedCallback() {
        std::cout << "[7] Buffer-finished callbacks" << std::endl;

        Context context(48000);
        auto data1 = MakePcmS16(1024);
        auto data2 = MakePcmS16(1024, 300);
        auto buffer1 = context.CreateBuffer(data1.data(), data1.size());
        auto buffer2 = context.CreateBuffer(data2.data(), data2.size());
        auto source = context.CreateSource(MakePcmDescription());

        FinishCounters finish;
        StateCounters state;
        source->SetBufferFinishedCallback(FinishCallback, &finish);
        source->SetStateChangedCallback(StateCallback, &state);

        source->SubmitBuffer(buffer1.get());
        source->SubmitBuffer(buffer2.get());
        source->Play();

        // Mix far past both buffers.
        MixFrames(context, 8192);

        // Exactly one buffer boundary was crossed (2 buffers -> 1 finish).
        CHECK(finish.Finished.load() == 1);
        CHECK(source->State() == SourceState::Stopped);
        CHECK(source->Position() == 0);
        CHECK(state.Stopped.load() == 1);
    }

    // ---------------------------------------------------------------------
    // 8. Concurrent stop/clear/destroy against a running mixer.
    // ---------------------------------------------------------------------
    void TestConcurrentTeardown() {
        std::cout << "[8] Concurrent stop / clear / destroy vs mixer" << std::endl;

        constexpr int kSources = 8;
        constexpr auto kDuration = std::chrono::milliseconds(1500);

        Context context(48000);

        struct Worker {
            std::unique_ptr<AudioBuffer> Buffer1;
            std::unique_ptr<AudioBuffer> Buffer2;
            std::unique_ptr<AudioSource> Source;
            StateCounters State;
            FinishCounters Finish;
        };

        std::vector<std::unique_ptr<Worker>> workers;
        for (int i = 0; i < kSources; i++) {
            auto worker = std::make_unique<Worker>();
            auto data1 = MakePcmS16(4096, static_cast<int16_t>(i * 7));
            auto data2 = MakePcmS16(4096, static_cast<int16_t>(i * 13));
            worker->Buffer1 = context.CreateBuffer(data1.data(), data1.size());
            worker->Buffer2 = context.CreateBuffer(data2.data(), data2.size());
            worker->Source = context.CreateSource(MakePcmDescription());
            worker->Source->SetStateChangedCallback(StateCallback, &worker->State);
            worker->Source->SetBufferFinishedCallback(FinishCallback, &worker->Finish);
            worker->Source->SubmitBuffer(worker->Buffer1.get());
            worker->Source->SubmitBuffer(worker->Buffer2.get());
            worker->Source->Play();
            workers.push_back(std::move(worker));
        }

        std::atomic<bool> running{true};

        // Mixer thread: continuously produces frames.
        std::thread mixer([&] {
            std::vector<float> out(512, 0.0f);
            while (running.load()) {
                context.MixToStereoF32Buffer(out.data(), out.size());
            }
        });

        // Control thread: randomly stop, clear, destroy buffers and sources,
        // then recreate them to exercise slot recycling under contention.
        std::thread controller([&] {
            auto deadline = std::chrono::steady_clock::now() + kDuration;
            unsigned seed = 12345;
            while (std::chrono::steady_clock::now() < deadline) {
                seed = seed * 1103515245u + 12345u;
                int index = (seed >> 16) % kSources;
                auto& worker = workers[index];
                if (!worker || !worker->Source)
                    continue;

                seed = seed * 1103515245u + 12345u;
                switch ((seed >> 16) % 5) {
                    case 0:
                        worker->Source->Stop();
                        break;
                    case 1:
                        worker->Source->ClearBuffers();
                        break;
                    case 2:
                        worker->Buffer1.reset();
                        break;
                    case 3: {
                        // Destroy and rebuild the whole source.
                        size_t id = worker->Source->ID();
                        worker->Source.reset();
                        auto fresh = context.CreateSource(MakePcmDescription());
                        CHECK(fresh->ID() == id);
                        fresh->SetStateChangedCallback(StateCallback, &worker->State);
                        fresh->SetBufferFinishedCallback(FinishCallback, &worker->Finish);
                        if (worker->Buffer1) fresh->SubmitBuffer(worker->Buffer1.get());
                        if (worker->Buffer2) fresh->SubmitBuffer(worker->Buffer2.get());
                        fresh->Play();
                        worker->Source = std::move(fresh);
                        break;
                    }
                    case 4:
                        if (worker->Source->State() == SourceState::Stopped) {
                            if (worker->Buffer1) worker->Source->SubmitBuffer(worker->Buffer1.get());
                            worker->Source->Play();
                        }
                        break;
                }
            }
        });

        controller.join();
        running = false;
        mixer.join();

        // Every surviving source must be in a coherent state.
        for (auto& worker : workers) {
            if (!worker || !worker->Source)
                continue;
            SourceState state = worker->Source->State();
            CHECK(state == SourceState::Stopped ||
                  state == SourceState::Paused ||
                  state == SourceState::Playing);
            if (state == SourceState::Stopped) {
                CHECK(worker->Source->Position() == 0);
            }
        }

        // One final mix pass must be safe after all the churn.
        MixFrames(context, 1024);
    }

    // ---------------------------------------------------------------------
    // 9. Re-entrant callbacks must not deadlock.
    // ---------------------------------------------------------------------
    struct ReentrantData {
        AudioSource* Source = nullptr;
        std::atomic<int> StopsSeen{0};
    };

    void ReentrantStateCallback(SourceState state, void* userData) {
        auto* data = static_cast<ReentrantData*>(userData);
        if (state == SourceState::Stopped) {
            data->StopsSeen++;
            // Calling back into the context from a callback must not deadlock.
            SourceState reentrantState = data->Source->State();
            (void) reentrantState;
            size_t reentrantPosition = data->Source->Position();
            (void) reentrantPosition;
        }
    }

    void TestReentrantCallback() {
        std::cout << "[9] Re-entrant state callback" << std::endl;

        Context context(48000);
        auto data = MakePcmS16(512);
        auto buffer = context.CreateBuffer(data.data(), data.size());
        auto source = context.CreateSource(MakePcmDescription());

        ReentrantData reentrant;
        reentrant.Source = source.get();
        source->SetStateChangedCallback(ReentrantStateCallback, &reentrant);

        source->SubmitBuffer(buffer.get());
        source->Play();

        // Natural stop fires the callback, which re-enters the context.
        MixFrames(context, 4096);

        CHECK(source->State() == SourceState::Stopped);
        CHECK(reentrant.StopsSeen.load() == 1);
    }

    // ---------------------------------------------------------------------
    // 10. Buffer slot reuse: a recycled buffer id carries no old payload.
    // ---------------------------------------------------------------------
    void TestBufferDestroyAndReuse() {
        std::cout << "[10] Buffer destroy and slot reuse" << std::endl;

        Context context(48000);

        size_t reusedId;
        {
            auto oldData = MakePcmS16(2048, 42);
            auto oldBuffer = context.CreateBuffer(oldData.data(), oldData.size());
            reusedId = oldBuffer->ID();
            oldBuffer.reset(); // slot parked
        }

        auto freshData = MakePcmS16(256, 999);
        auto freshBuffer = context.CreateBuffer(freshData.data(), freshData.size());
        CHECK(freshBuffer->ID() == reusedId);

        auto source = context.CreateSource(MakePcmDescription());
        source->SubmitBuffer(freshBuffer.get());
        source->Play();

        // The source must measure the *new* (short) buffer, not the old 2048-sample
        // payload, and must stop naturally once the new data is consumed.
        MixFrames(context, 4096);
        CHECK(source->State() == SourceState::Stopped);
        CHECK(source->Position() == 0);
    }

    // ---------------------------------------------------------------------
    // 11. Streaming: re-queue a recycled buffer from the finish callback.
    // ---------------------------------------------------------------------
    struct StreamingData {
        AudioSource* Source = nullptr;
        AudioBuffer* BufferA = nullptr;
        AudioBuffer* BufferB = nullptr;
        std::vector<uint8_t> Data;
        std::atomic<int> Finishes{0};
        int Requeues = 0;
    };

    void StreamingFinishCallback(void* userData) {
        auto* data = static_cast<StreamingData*>(userData);
        data->Finishes++;

        // Re-queue the alternating buffer, as a real streaming user would.
        AudioBuffer* next = (data->Requeues % 2 == 0) ? data->BufferA : data->BufferB;
        data->Source->SubmitBuffer(next);
        data->Requeues++;
    }

    void TestStreamingRequeueFromCallback() {
        std::cout << "[11] Re-queue from buffer-finished callback" << std::endl;

        Context context(48000);
        auto dataA = MakePcmS16(512, 10);
        auto dataB = MakePcmS16(512, 20);
        auto bufferA = context.CreateBuffer(dataA.data(), dataA.size());
        auto bufferB = context.CreateBuffer(dataB.data(), dataB.size());
        auto source = context.CreateSource(MakePcmDescription());

        StreamingData streaming;
        streaming.Source = source.get();
        streaming.BufferA = bufferA.get();
        streaming.BufferB = bufferB.get();

        source->SetBufferFinishedCallback(StreamingFinishCallback, &streaming);

        // Simulate a device callback: each mix pass is short. A real streaming
        // user keeps a few buffers queued; the finish callback recycles consumed
        // buffers back onto the queue between mix passes.
        source->SubmitBuffer(bufferA.get());
        source->SubmitBuffer(bufferB.get());
        source->Play();

        for (int pass = 0; pass < 8; pass++) {
            MixFrames(context, 256); // 256 frames < 512-sample buffers

            if (source->State() == SourceState::Stopped) {
                // The mixer drained everything queued so far; top up again,
                // exactly as a streaming pump would, and resume.
                source->SubmitBuffer(bufferA.get());
                source->SubmitBuffer(bufferB.get());
                source->Play();
            }
        }

        // The finish callback fired on every consumed buffer and each one
        // re-queued a valid buffer; nothing touched a destroyed buffer.
        CHECK(streaming.Finishes.load() >= 3);
        CHECK(streaming.Requeues == streaming.Finishes.load());
        CHECK(source->State() == SourceState::Playing);

        source->Stop();
        CHECK(source->State() == SourceState::Stopped);
        CHECK(source->Position() == 0);
    }

} // namespace

int main() {
    std::cout << "Running mixr++ core concurrency tests..." << std::endl;

    TestStopDuringPlayback();
    TestClearBuffers();
    TestDestroyFrontBuffer();
    TestDestroyQueuedBuffer();
    TestSourceDestroyAndReuse();
    TestStateNotificationDedup();
    TestBufferFinishedCallback();
    TestConcurrentTeardown();
    TestReentrantCallback();
    TestBufferDestroyAndReuse();
    TestStreamingRequeueFromCallback();

    std::cout << std::endl;
    std::cout << g_checks << " checks, " << g_failures << " failures" << std::endl;
    return g_failures == 0 ? 0 : 1;
}
