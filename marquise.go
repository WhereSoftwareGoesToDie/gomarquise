/*
The gomarquise package consists of a set of bindings (using CGo) for the
libmarquise[0] metric writer library.

[0] https://github.com/anchor/libmarquise
*/
package gomarquise

import (
	"fmt"
	"unsafe"
)

// #include <marquise.h>
// #include <stdint.h>
// #include <stdlib.h>
// #cgo LDFLAGS: -lmarquise
import "C"

const (
	Version = "2.0.0alpha1"
)

// Maintains the ZeroMQ context.
type MarquiseContext struct {
	ctx *C.marquise_ctx
}

func newMarquiseWriteError(ret int, value string) error {
	return fmt.Errorf("libmarquise returned %v whilst trying to write frame with value %v", ret, value)
}

func newMarquiseContextError(msg string) error {
	return fmt.Errorf("Error initializing libmarquise context: %v", msg)
}

// NewMarquiseContext takes a string representing the Marquise
// namespace (this must be unique per-host). 
//
// Wraps C functions from marquise.h:
//
// - marquise_init
func NewMarquiseContext(namespace string) (*MarquiseContext, error) {
	context := new(MarquiseContext)
	ns := C.CString(namespace)
	defer C.free(unsafe.Pointer(ns))
	context.ctx = C.marquise_init(ns)
	if context.ctx == nil {
		return nil, newMarquiseContextError(fmt.Sprintf("marquise_init(%v) returned NULL", namespace))
	}
	return context, nil
}

// Shutdown flushes and closes the spool.
//
// Wraps C functions from marquise.h:
//
// - marquise_shutdown
func (c MarquiseContext) Shutdown() {
	C.marquise_shutdown(c.ctx)
}

