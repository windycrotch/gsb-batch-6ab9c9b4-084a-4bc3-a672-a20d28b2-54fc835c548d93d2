package adler

import (
	"time"

	"github.com/lxzan/gws"
)

type message struct {
	messageType gws.Opcode
	content     []byte
	filter      filterFunc
	writeWait   time.Duration
}
