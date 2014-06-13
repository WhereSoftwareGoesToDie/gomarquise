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
		return nil, fmt.Errorf("marquise_init(%v) returned NULL", namespace)
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

// Return the SipHash-2-4[0] of the supplied identifier string (must be
// unique per-origin).
//
// Wraps C functions from marquise.h:
//
// - marquise_hash_identifier
//
// [0] https://131002.net/siphash/
func HashIdentifier(id string) uint64 {
	id_ := C.CString(id)
	defer C.free(unsafe.Pointer(id_))
	idLen := C.size_t(len(id))
	return uint64(C.marquise_hash_identifier(id_, idLen))
}

// SendSimple queues a word64 datapoint for transmission by the 
// Marquise daemon. address is the value returned by HashIdentifier.
//
// Wraps C functions from marquise.h:
//
// - marquise_send_simple
func (c MarquiseContext) SendSimple(address, timestamp, value uint64) error {
	ret := C.marquise_send_simple(c.ctx, C.uint64_t(address), C.uint64_t(timestamp), C.uint64_t(value))
	if ret != 0 {
		return fmt.Errorf("marquise_send_simple(%v, %v, %v, %v) returned %v", c.ctx, address, timestamp, value)
	}
	return nil
}

// SendExtended queues a string datapoint for transmission by the
// Marquise daemon. address is the value returned by HashIdentifier.
//
// Wraps C functions from marquise.h:
//
// - marquise_send_extended
func (c MarquiseContext) SendExtended(address, timestamp uint64, value string) error {
	cVal := C.CString(value)
	defer C.free(unsafe.Pointer(cVal))
	cLen := C.size_t(len(value))
	ret := C.marquise_send_extended(c.ctx, C.uint64_t(address), C.uint64_t(timestamp), cVal, cLen)
	if ret != 0 {
		return fmt.Errorf("marquise_send_extended(%v, %v, %v, %v, %v) returned %v", c.ctx, address, timestamp, value, len(value))
	}
	return nil
}

// Flush ensures written datapoints are written to disk. This is just
// a wrapper for fflush(2) and you probably don't need to call it.
//
// Wraps C functions from marquise.h:
//
// - marquise_flush
func (c MarquiseContext) Flush() error {
	ret := C.marquise_flush(c.ctx)
	if ret != 0 {
		return fmt.Errorf("marquise_flush() returned %v", ret)
	}
	return nil
}
