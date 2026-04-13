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
	"io"
	"time"
)

// writeDeadliner is satisfied by net.Conn. It is defined separately so that
// writeLoop remains testable with plain io.Writer implementations such as
// *bytes.Buffer.
type writeDeadliner interface {
	SetWriteDeadline(t time.Time) error
}

// writeLoop consumes outgoing messages from messageOut and writes them to
// connection. When writeTimeout > 0 and connection supports deadlines, each
// Write is bounded by the deadline so that an unresponsive peer cannot block
// the loop indefinitely (for instance when the OS TCP send buffer fills on a
// half-open connection).
//
// When a write returns an error, writeLoop spawns a goroutine that drains
// messageOut and then returns. Draining is required because the session's
// run() goroutine may be blocked on a synchronous send to messageOut (e.g.
// sendBytes(msg, true) for heartbeats and test requests); without draining,
// the session would remain deadlocked and could not process the subsequent
// disconnect event. Returning from writeLoop lets the caller close the
// underlying connection, which in turn unblocks readLoop and triggers the
// session's normal disconnect handling.
func writeLoop(connection io.Writer, messageOut chan []byte, log Log, writeTimeout time.Duration) {
	deadliner, canDeadline := connection.(writeDeadliner)

	for {
		msg, ok := <-messageOut
		if !ok {
			return
		}

		if canDeadline && writeTimeout > 0 {
			_ = deadliner.SetWriteDeadline(time.Now().Add(writeTimeout))
		}

		if _, err := connection.Write(msg); err != nil {
			log.OnEvent(err.Error())
			go drainMessageOut(messageOut)
			return
		}
	}
}

// drainMessageOut discards messages pushed to messageOut until it is closed.
// It unblocks any goroutine synchronously sending to messageOut (typically
// the session's run goroutine in sendBytes) so the session can proceed to its
// disconnect handling, which is what ultimately closes messageOut.
func drainMessageOut(messageOut chan []byte) {
	for range messageOut {
	}
}

func readLoop(parser *parser, msgIn chan fixIn, log Log) {
	defer close(msgIn)

	for {
		msg, err := parser.ReadMessage()
		if err != nil {
			log.OnEvent(err.Error())
			return
		}
		msgIn <- fixIn{msg, parser.lastRead}
	}
}
