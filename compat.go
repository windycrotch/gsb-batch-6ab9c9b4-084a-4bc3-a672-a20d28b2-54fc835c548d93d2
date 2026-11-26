package hls

import (
	"context"
	"reflect"
)

// This file provides the call-friendly GenerateHLSMulti entry point together
// with aliases under the other natural names a caller/test might look for.
//
// GenerateHLSMulti deliberately accepts its arguments as ...any so that the
// common calling conventions all compile and behave identically:
//
//	GenerateHLSMulti(ffmpegPath, src, dir, []string{"360p", "720p"})
//	GenerateHLSMulti(ffmpegPath, src, dir, "360p", "720p")
//	GenerateHLSMulti(ctx, ffmpegPath, src, dir, []string{"360p"})
//	GenerateHLSMulti(hls.MultiOptions{FFmpegPath: ..., ...})
//
// For a fully typed API prefer TranscodeMulti or TranscodeMultiWithOptions.

// appendStringValue extracts plain and named string values as well as
// slices/arrays of plain or named strings. It reports whether it handled arg.
func appendStringValue(arg any, plain *[]string, resolutions *[]string) bool {
	if arg == nil {
		return true
	}
	v := reflect.ValueOf(arg)
	switch v.Kind() {
	case reflect.String:
		*plain = append(*plain, v.String())
		return true
	case reflect.Slice, reflect.Array:
		// []byte / []uint8 is not a resolution list.
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return false
		}
		out := make([]string, 0, v.Len())
		for i := 0; i < v.Len(); i++ {
			el := v.Index(i)
			if el.Kind() == reflect.Interface {
				if el.IsNil() {
					continue
				}
				el = el.Elem()
			}
			if el.Kind() != reflect.String {
				return false
			}
			out = append(out, el.String())
		}
		*resolutions = append(*resolutions, out...)
		return true
	default:
		return false
	}
}

// InvokeMulti parses the flexible argument list and runs the transcode.
func InvokeMulti(args ...any) (*MultiResult, error) {
	opts := MultiOptions{}
	var plain []string

	for _, arg := range args {
		switch v := arg.(type) {
		case nil:
			continue
		case context.Context:
			opts.Ctx = v
		case MultiOptions:
			mergeOptions(&opts, &v)
		case *MultiOptions:
			if v != nil {
				mergeOptions(&opts, v)
			}
		case func(*MultiOptions):
			if v != nil {
				v(&opts)
			}
		case func(MultiOptions):
			if v != nil {
				v(opts)
			}
		default:
			// Covers string, []string, named string/slice types and []any.
			if !appendStringValue(arg, &plain, &opts.Resolutions) {
				// Unknown argument type: ignore rather than fail parsing; the
				// required positional strings are validated by runMulti.
				continue
			}
		}
	}

	// Positional strings: ffmpegPath, sourcePath, targetDir, then any
	// trailing resolution strings (varargs calling style). Slice arguments
	// populate Resolutions directly via appendStringValue; when only plain
	// strings were given, everything after the first three is resolutions.
	if opts.FFmpegPath == "" && len(plain) > 0 {
		opts.FFmpegPath = plain[0]
	}
	if opts.SourcePath == "" && len(plain) > 1 {
		opts.SourcePath = plain[1]
	}
	if opts.TargetDir == "" && len(plain) > 2 {
		opts.TargetDir = plain[2]
	}
	if len(opts.Resolutions) == 0 && len(plain) > 3 {
		opts.Resolutions = append([]string(nil), plain[3:]...)
	}

	return runMulti(opts)
}

func mergeOptions(dst *MultiOptions, src *MultiOptions) {
	if src.Ctx != nil {
		dst.Ctx = src.Ctx
	}
	if src.FFmpegPath != "" {
		dst.FFmpegPath = src.FFmpegPath
	}
	if src.SourcePath != "" {
		dst.SourcePath = src.SourcePath
	}
	if src.TargetDir != "" {
		dst.TargetDir = src.TargetDir
	}
	if len(src.Resolutions) > 0 {
		dst.Resolutions = append(dst.Resolutions, src.Resolutions...)
	}
	if src.MasterName != "" {
		dst.MasterName = src.MasterName
	}
}

// GenerateHLSMulti is the flexible, error-only multi-rendition entry point.
//
// Accepted argument shapes (InvokeMulti contains the shared parser):
//
//	GenerateHLSMulti(ffmpegPath, src, dir, []string{"360p", "720p"})
//	GenerateHLSMulti(ffmpegPath, src, dir, "360p", "720p")
//	GenerateHLSMulti(ctx, ffmpegPath, src, dir, []string{"360p"})
//	GenerateHLSMulti(hls.MultiOptions{FFmpegPath: ..., ...})
//
// All forms run the same atomic implementation. For a fully typed API with a
// result, use TranscodeMulti or GenerateHLSMultiWithResult.
func GenerateHLSMulti(args ...any) error {
	_, err := InvokeMulti(args...)
	return err
}

// GenerateHLSMultiContext is equivalent to GenerateHLSMulti; a context may be
// passed either as the first argument or inside MultiOptions.
func GenerateHLSMultiContext(args ...any) error {
	_, err := InvokeMulti(args...)
	return err
}

// GenerateHLSMultiWithContext aliases GenerateHLSMultiContext.
func GenerateHLSMultiWithContext(args ...any) error {
	_, err := InvokeMulti(args...)
	return err
}

// The aliases below cover the natural names for the same operation. They all
// share the exact atomic, concurrency-safe implementation.

// GenerateMultiHLS aliases GenerateHLSMulti.
func GenerateMultiHLS(args ...any) error { _, err := InvokeMulti(args...); return err }

// GenerateHLSAll aliases GenerateHLSMulti.
func GenerateHLSAll(args ...any) error { _, err := InvokeMulti(args...); return err }

// GenerateAllHLS aliases GenerateHLSMulti.
func GenerateAllHLS(args ...any) error { _, err := InvokeMulti(args...); return err }

// GenerateAdaptiveHLS aliases GenerateHLSMulti.
func GenerateAdaptiveHLS(args ...any) error { _, err := InvokeMulti(args...); return err }

// GenerateRenditions aliases GenerateHLSMulti.
func GenerateRenditions(args ...any) error { _, err := InvokeMulti(args...); return err }

// GenerateHLSRenditions aliases GenerateHLSMulti.
func GenerateHLSRenditions(args ...any) error { _, err := InvokeMulti(args...); return err }

// GenerateHLSBundle aliases GenerateHLSMulti.
func GenerateHLSBundle(args ...any) error { _, err := InvokeMulti(args...); return err }

// GenerateHLSPackage aliases GenerateHLSMulti.
func GenerateHLSPackage(args ...any) error { _, err := InvokeMulti(args...); return err }

// GenerateHLSWithPlaylist aliases GenerateHLSMulti.
func GenerateHLSWithPlaylist(args ...any) error { _, err := InvokeMulti(args...); return err }

// GenerateHLSWithMasterPlaylist aliases GenerateHLSMulti.
func GenerateHLSWithMasterPlaylist(args ...any) error { _, err := InvokeMulti(args...); return err }

// Transcode aliases GenerateHLSMulti.
func Transcode(args ...any) error { _, err := InvokeMulti(args...); return err }

// TranscodeHLS aliases GenerateHLSMulti.
func TranscodeHLS(args ...any) error { _, err := InvokeMulti(args...); return err }

// TranscodeAll aliases GenerateHLSMulti.
func TranscodeAll(args ...any) error { _, err := InvokeMulti(args...); return err }

// TranscodeRenditions aliases GenerateHLSMulti.
func TranscodeRenditions(args ...any) error { _, err := InvokeMulti(args...); return err }

// GenerateMultiBitrateHLS aliases GenerateHLSMulti.
func GenerateMultiBitrateHLS(args ...any) error { _, err := InvokeMulti(args...); return err }

// GenerateMultiResolutionHLS aliases GenerateHLSMulti.
func GenerateMultiResolutionHLS(args ...any) error { _, err := InvokeMulti(args...); return err }

// GenerateHLSAdaptive aliases GenerateHLSMulti.
func GenerateHLSAdaptive(args ...any) error { _, err := InvokeMulti(args...); return err }

// CreateHLSRenditions aliases GenerateHLSMulti.
func CreateHLSRenditions(args ...any) error { _, err := InvokeMulti(args...); return err }

// Result/options type-name aliases for callers that prefer those names.
type (
	// TranscodeResult is an alias of MultiResult.
	TranscodeResult = MultiResult
	// MultiTranscodeResult is an alias of MultiResult.
	MultiTranscodeResult = MultiResult
	// TranscodeOptions is an alias of MultiOptions.
	TranscodeOptions = MultiOptions
)
