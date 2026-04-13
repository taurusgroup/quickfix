// Copyright (c) quickfixengine.org  All rights reserved.
//
// This file may be distributed under the terms of the quickfixengine.org
// license as defined by quickfixengine.org and appearing in the file
// LICENSE included in the packaging of this file.
//
// This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING
// THE WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A
// PARTICULAR PURPOSE.
//
// See http://www.quickfixengine.org/LICENSE for licensing information.
//
// Contact ask@quickfixengine.org if any conditions of this licensing
// are not clear to you.

package quickfix

import (
	"bytes"
	"net"
	"strings"
	"testing"
	"time"
)

func TestWriteLoop(t *testing.T) {
	writer := bytes.NewBufferString("")
	msgOut := make(chan []byte)

	go func() {
		msgOut <- []byte("test msg 1 ")
		msgOut <- []byte("test msg 2 ")
		msgOut <- []byte("test msg 3")
		close(msgOut)
	}()
	writeLoop(writer, msgOut, nullLog{}, 0)

	expected := "test msg 1 test msg 2 test msg 3"

	if writer.String() != expected {
		t.Errorf("expected %v got %v", expected, writer.String())
	}
}

func TestWriteLoop_AppliesWriteDeadline(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	msgOut := make(chan []byte, 1)
	msgOut <- []byte("test")
	// Do not close msgOut — we expect writeLoop to return on write error.

	done := make(chan struct{})
	go func() {
		writeLoop(server, msgOut, nullLog{}, 50*time.Millisecond)
		close(done)
	}()

	select {
	case <-done:
		// writeLoop returned after deadline fired on net.Pipe (nobody reads)
	case <-time.After(2 * time.Second):
		t.Fatal("writeLoop did not return after write deadline expired")
	}
}

func TestWriteLoop_DrainsMessageOutAfterError(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	msgOut := make(chan []byte)

	// Kick off writeLoop; the first write will time out because net.Pipe
	// has no buffering and nobody is reading from client.
	writeLoopDone := make(chan struct{})
	go func() {
		writeLoop(server, msgOut, nullLog{}, 50*time.Millisecond)
		close(writeLoopDone)
	}()

	msgOut <- []byte("first") // triggers Write, which times out
	<-writeLoopDone

	// Subsequent synchronous sends must not block — the drain goroutine
	// should be receiving them.
	sent := make(chan struct{})
	go func() {
		msgOut <- []byte("after-error")
		close(sent)
	}()

	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("post-error send blocked — drain goroutine not running")
	}

	// Closing messageOut terminates the drain goroutine.
	close(msgOut)
}

func TestReadLoop(t *testing.T) {
	msgIn := make(chan fixIn)
	stream := "hello8=FIX.4.09=5blah10=103garbage8=FIX.4.09=4foo10=103"

	parser := newParser(strings.NewReader(stream))
	go readLoop(parser, msgIn, nullLog{})

	var tests = []struct {
		expectedMsg   string
		channelClosed bool
	}{
		{expectedMsg: "8=FIX.4.09=5blah10=103"},
		{expectedMsg: "8=FIX.4.09=4foo10=103"},
		{channelClosed: true},
	}

	for _, test := range tests {
		msg, ok := <-msgIn
		switch {
		case !ok && !test.channelClosed:
			t.Error("Channel unexpectedly closed")
			fallthrough
		case !ok && test.channelClosed:
			continue
		}

		if msg.bytes.String() != test.expectedMsg {
			t.Errorf("Expected %v got %v", test.expectedMsg, msg.bytes.String())
		}
	}
}
