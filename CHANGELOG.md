# 更新日志（CHANGELOG）

本文件记录 audio-cpp-rs 每个版本面向使用者的变化。格式参照 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)：

- 新增条目写入「未发布」段，随代码一起提交；
- 发布时把「未发布」段改名对应版本号并补日期（版本号与工作区 `Cargo.toml` 一致），再打 `v<版本>` 标签；
- 打标签后预编译工作流会自动提取 CHANGELOG 中对应版本段作为 Release 说明，条目请面向使用者描述变化，不写内部实现细节；
- 0.10.0 之前的历史版本未维护本文件，不补记。

## [未发布]

### 新增

- 跟进上游 audio.cpp（submodule 从 `5e7273db` 前进到 `d09b2d78`），新增 4 个模型族（全量 loader 族 151 → 155，完整清单见 [模型目录](docs/models.md)）：
  - `dettato`：Dettato 28M 多乐器音频转 MIDI（音符检测 + 证据修复 + 乐器分组 + 鼓转录；task `midi` 仅离线；feature `model-dettato`）。
  - `kale_tts`：kale-tts-1 nano/tiny 英文 TTS，10 个预置音色、自带词表前端（task `tts` 仅离线；feature `model-kale-tts`）。
  - `voice_isolator`：Inworld Voice Isolator v1 48kHz 人声隔离，realtime 检查点支持流式（task `s2s`，离线+流式；feature `model-voice-isolator`）。
  - `voxmel_tts`：VoxMel 0.6B 连续自回归 TTS + 参考音频克隆（task `tts`/`clon` 仅离线，en/zh；feature `model-voxmel-tts`）。
- `ModelFamily` 新增对应枚举变体与 `from_path()` 识别；`TaskKind` 无需变化（以上复用既有 `midi`/`tts`/`clon`/`s2s`）。

### 变更

- 请求选项 `seed=-1` 现按上游统一语义解析为每次请求随机的整数（`0..2147483647`），与 CLI/server/C API 一致；prepare 与随后的 run 使用同一次解析结果。
- 上游行为随 submodule 升级带入：Silero VAD 离线推理接受任意采样率；KittenTTS 2 Mini 与新音色、Phonon 40M 法语/多语包、服务器 CLI 配置覆盖、chinese 文本与 NEON iSTFT 修复等；选项均经通用字符串透传，serde 结构向后兼容。`kale_tts` 与 `kitten_tts` 的 `phonemes` 为数组选项（`string_list`），本仓 shim 暂不支持数组选项（与既有 `kokoro` 缺口相同）。

## [0.10.0] - 2026-11-20

### 新增

- 跟进上游 audio.cpp（submodule 从 `d2da530b` 前进到 `5e7273db`），新增 5 个模型族（全量 loader 族 147 → 151，完整清单见 [模型目录](docs/models.md)）：
  - `m5_tts`：M5-TTS 多语自回归 TTS + 参考音频克隆（Qwen3 speech codec 解码；zh/yue/zh-Sichuan/en/de/fr/ja/ko；离线+流式；feature `model-m5-tts`）。
  - `tiny_voca`：Tiny Voca 4.5M 参数英文 TTS，自带 dizione 音素化前端、无需外部 phonemizer，内置 M2/F2 两音色，支持长文本（仅离线；feature `model-tiny-voca`）。
  - `rosvot`：ROSVOT 歌声转符号谱——音符音高/时值与词对齐 MIDI 转写，输出 MIDI 或 JSON 音符事件（仅离线；feature `model-rosvot`）。
  - `soulx_singer`：SoulX-Singer 参考音频条件的歌唱合成（SVS）与歌唱 voice conversion（SVC），zh/en/yue（仅离线；task `music`/`svc`；feature `model-soulx-singer`）。
  - `granite_speech5`：IBM Granite Speech 5.0 2B 自回归多语 ASR + 语音翻译，en/fr/de/es/pt/ja，翻译目标另支持 it/zh（仅离线；feature `model-granite-speech5`）。
- `ModelFamily` 新增对应枚举变体与 `from_path()` 识别，`TaskKind` 无需变化（`svc` 为既有任务类型）。

### 变更

- 上游模型行为随 submodule 升级带入：`zonos2` 新增长文本续写选项 `long_form_continuation` 且分块默认值 300→150；`cosyvoice3` 新增 `speed` 语速选项；`ace_step` 新增 `reference_audio` 音色条件；`granite5asr` 编码器图 arena 默认值调整。全部经通用 options 字符串透传，`audio-cpp` 的 serde 结构向后兼容。
- 推送 `v*` 标签触发的预编译工作流现在会校验标签与 `Cargo.toml` 版本一致，并从本文件提取对应版本段作为 GitHub Release 说明（缺失时退回通用文案并告警）。
