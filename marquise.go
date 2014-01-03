package gomarquise

import (
	"fmt"
	"unsafe"
)

// #include <marquise.h>
// #include <stdint.h>
// #cgo LDFLAGS: -lmarquise
import "C"

const (
	Version = "1.0.4"
)

// Maintains the ZeroMQ context
type MarquiseContext struct {
	consumer   C.marquise_consumer
	connection C.marquise_connection
}

func newMarquiseWriteError(value string) error {
	return fmt.Errorf("libmarquise returned -1 whilst trying to write frame with value %v", value)
}

func newMarquiseContextError(msg string) error {
	return fmt.Errorf("Error initializing libmarquise context: %v", msg)
}

// zmqBroker is a string taking the form of a ZeroMQ URI. batchPeriod
// is the interval at which the worker thread will poll/empty the queue
// of messages.
//
// Wraps C functions from anchor_stats.h:
//
// - as_consumer_new
// - as_connect
func Dial(zmqBroker string, batchPeriod float64) (MarquiseContext, error) {
	context := new(MarquiseContext)
	broker := C.CString(zmqBroker)
	defer C.free(unsafe.Pointer(broker))
	interval := C.double(batchPeriod)
	context.consumer = C.marquise_consumer_new(broker, interval)
	if context.consumer == nil {
		return *context, newMarquiseContextError(fmt.Sprintf("as_consumer_new(%v, %v) returned NULL", broker, interval))
	}
	context.connection = C.marquise_connect(context.consumer)
	if context.connection == nil {
		return *context, newMarquiseContextError(fmt.Sprintf("as_connect(%v) returned NULL", context.consumer))
	}
	return *context, nil
}

func (c MarquiseContext) Shutdown() {
	C.marquise_consumer_shutdown(c.consumer)
}

// Translates a map of source tags to an array of CStrings of fields,
// an array of CStrings of values and a size_t of the number of pairs.
//
// You need to free the two arrays of CStrings in the calling code,
// after you've finished using them.
func translateSource(source map[string]string) ([]*C.char, []*C.char, C.size_t) {
	nTags := len(source)
	tagFields := make([]*C.char, nTags)
	tagValues := make([]*C.char, nTags)
	idx := 0
	for field, value := range source {
		tagFields[idx] = C.CString(field)
		tagValues[idx] = C.CString(value)
		idx += 1
	}
	return tagFields, tagValues, C.size_t(nTags)
}

// Write a (UTF8) string value.
func (c MarquiseContext) WriteText(source map[string]string, data string, timestamp uint64) error {
	tagFields, tagValues, tagCount := translateSource(source)
	for idx, _ := range tagFields {
		defer C.free(unsafe.Pointer(tagFields[idx]))
		defer C.free(unsafe.Pointer(tagValues[idx]))
	}
	cFields := &tagFields[0]
	cValues := &tagValues[0]
	cStr := C.CString(data)
	defer C.free(unsafe.Pointer(cStr))
	cLen := C.size_t(len(data))
	cTimestamp := C.uint64_t(timestamp)
	writeResult := C.marquise_send_text(c.connection, cFields, cValues, tagCount, cStr, cLen, cTimestamp)
	if writeResult == -1 {
		return newMarquiseWriteError(data)
	}
	return nil
}

// Write a 64-bit int value.
func (c MarquiseContext) WriteInt(source map[string]string, data int64, timestamp uint64) error {
	tagFields, tagValues, tagCount := translateSource(source)
	for idx, _ := range tagFields {
		defer C.free(unsafe.Pointer(tagFields[idx]))
		defer C.free(unsafe.Pointer(tagValues[idx]))
	}
	cFields := &tagFields[0]
	cValues := &tagValues[0]
	cInt := C.int64_t(data)
	cTimestamp := C.uint64_t(timestamp)
	writeResult := C.marquise_send_int(c.connection, cFields, cValues, tagCount, cInt, cTimestamp)
	if writeResult == -1 {
		return newMarquiseWriteError(fmt.Sprintf("%v", data))
	}
	return nil
}

// Write a 64-bit float value.
func (c MarquiseContext) WriteReal(source map[string]string, data float64, timestamp uint64) error {
	tagFields, tagValues, tagCount := translateSource(source)
	for idx, _ := range tagFields {
		defer C.free(unsafe.Pointer(tagFields[idx]))
		defer C.free(unsafe.Pointer(tagValues[idx]))
	}
	cFields := &tagFields[0]
	cValues := &tagValues[0]
	cFloat := C.double(data)
	cTimestamp := C.uint64_t(timestamp)
	writeResult := C.marquise_send_real(c.connection, cFields, cValues, tagCount, cFloat, cTimestamp)
	if writeResult == -1 {
		return newMarquiseWriteError(fmt.Sprintf("%v", data))
	}
	return nil
}

// Write an empty/'counter' value.
func (c MarquiseContext) WriteCounter(source map[string]string, timestamp uint64) error {
	tagFields, tagValues, tagCount := translateSource(source)
	for idx, _ := range tagFields {
		defer C.free(unsafe.Pointer(tagFields[idx]))
		defer C.free(unsafe.Pointer(tagValues[idx]))
	}
	cFields := &tagFields[0]
	cValues := &tagValues[0]
	cTimestamp := C.uint64_t(timestamp)
	writeResult := C.marquise_send_counter(c.connection, cFields, cValues, tagCount, cTimestamp)
	if writeResult == -1 {
		return newMarquiseWriteError("<EMPTY>")
	}
	return nil
}

// Write a binary blob/byte array.
func (c MarquiseContext) WriteBinary(source map[string]string, data []byte, timestamp uint64) error {
	tagFields, tagValues, tagCount := translateSource(source)
	for idx, _ := range tagFields {
		defer C.free(unsafe.Pointer(tagFields[idx]))
		defer C.free(unsafe.Pointer(tagValues[idx]))
	}
	cFields := &tagFields[0]
	cValues := &tagValues[0]
	cTimestamp := C.uint64_t(timestamp)
	// FIXME: do less wrong
	buf := make([]C.uint8_t, len(data))
	for i, val := range data {
		buf[i] = C.uint8_t(val)
	}
	nBytes := C.size_t(len(data))
	writeResult := C.marquise_send_binary(c.connection, cFields, cValues, tagCount, &buf[0], nBytes, cTimestamp)
	if writeResult == -1 {
		return newMarquiseWriteError(fmt.Sprintf("%v", data))
	}
	return nil
}
