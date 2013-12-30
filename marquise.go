package gomarquise

import (
	"fmt"
	"unsafe"
)

// #include <anchor_stats.h>
// #include <stdint.h>
// #cgo LDFLAGS: -lanchor_stats
import "C"

type MarquiseContext struct {
	consumer   C.as_consumer
	connection C.as_connection
}

func newMarquiseWriteError(value string) error {
	return fmt.Errorf("libmarquise returned -1 whilst trying to write frame with value %v", value)
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
	context.consumer = C.as_consumer_new(broker, interval)
	context.connection = C.as_connect(context.consumer)
	return *context, nil
}

// Need to free tagFields and tagValues as they're malloced
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
	writeResult := C.as_send_text(c.connection, cFields, cValues, tagCount, cStr, cLen, cTimestamp)
	if writeResult == -1 {
		return newMarquiseWriteError(data)
	}
	return nil
}

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
	writeResult := C.as_send_int(c.connection, cFields, cValues, tagCount, cInt, cTimestamp)
	if writeResult == -1 {
		return newMarquiseWriteError(fmt.Sprintf("%v", data))
	}
	return nil
}

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
	writeResult := C.as_send_real(c.connection, cFields, cValues, tagCount, cFloat, cTimestamp)
	if writeResult == -1 {
		return newMarquiseWriteError(fmt.Sprintf("%v", data))
	}
	return nil
}

func (c MarquiseContext) WriteCounter(source map[string]string, timestamp uint64) error {
	tagFields, tagValues, tagCount := translateSource(source)
	for idx, _ := range tagFields {
		defer C.free(unsafe.Pointer(tagFields[idx]))
		defer C.free(unsafe.Pointer(tagValues[idx]))
	}
	cFields := &tagFields[0]
	cValues := &tagValues[0]
	cTimestamp := C.uint64_t(timestamp)
	writeResult := C.as_send_counter(c.connection, cFields, cValues, tagCount, cTimestamp)
	if writeResult == -1 {
		return newMarquiseWriteError("<EMPTY>")
	}
	return nil
}


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
	writeResult := C.as_send_binary(c.connection, cFields, cValues, tagCount, &buf[0], nBytes, cTimestamp)
	if writeResult == -1 {
		return newMarquiseWriteError(fmt.Sprintf("%v", data))
	}
	return nil
}

