package gomarquise

import (
	zmq "github.com/pebbe/zmq4"
	"testing"
	"time"
)

func sendTestMessage(t *testing.T, v int) {
	c, err := Dial("ipc:///tmp/gomarquise_full_stack_test", 0.1, "ABCDEF", "", true)
	if err != nil {
		t.Errorf("%v", err)
	}
	m := make(map[string]string, 0)
	m["foo"] = "bar"
	err = c.WriteInt(m, int64(42), uint64(time.Now().UnixNano()))
	if err != nil {
		t.Errorf("%v", err)
	}
	c.Shutdown()
}

// TODO: actually decode message
func TestMarquiseOneMessage(t *testing.T) {
	sock, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		t.Errorf("%v", err)
	}
	err = sock.Bind("ipc:///tmp/gomarquise_full_stack_test")
	if err != nil {
		t.Errorf("%v", err)
	}
	go sendTestMessage(t, 42)
	_, err = sock.RecvMessageBytes(0)
	if err != nil {
		t.Errorf("%v", err)
	}
}
