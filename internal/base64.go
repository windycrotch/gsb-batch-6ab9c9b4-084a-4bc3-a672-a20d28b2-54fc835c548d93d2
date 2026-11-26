package internal

import (
	"context"
	"encoding/base64"
	"os"
)

// WriteBase64ToFile decodes data into path. It honors cancellation before
// decoding and again before writing, so a canceled request does not leave an
// input file that a later step might consume.
func WriteBase64ToFile(ctx context.Context, data, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	bytes, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.WriteFile(path, bytes, 0644)
}
