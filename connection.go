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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

func writeLoop(connection io.Writer, messageOut chan []byte, log Log) {
	for {
		msg, ok := <-messageOut
		if !ok {
			return
		}

		if _, err := connection.Write(msg); err != nil {
			log.OnEvent(err.Error())
		}
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

// connWriter returns the writer writeLoop sends on. With no SocketWriteTimeout it is the connection
// itself, exactly as before the setting existed.
func connWriter(conn net.Conn, writeTimeout time.Duration, log Log) io.Writer {
	if writeTimeout <= 0 {
		return conn
	}

	return &deadlineWriter{conn: conn, timeout: writeTimeout, log: log}
}

// deadlineWriter bounds each Write by a deadline and closes the connection when one expires.
//
// A write to a counterparty that has stopped reading otherwise blocks indefinitely. messageOut is
// unbuffered, so the session goroutine's next admin send, and any application send holding the
// session's sendMutex, then blocks behind it, and the session can no longer act on its own timers or
// a disconnect. Closing the connection makes the reader fail, so the session sees a disconnect, and
// writeLoop keeps draining messageOut (each later write fails at once on the closed connection) until
// the session closes it, which releases every blocked sender. A timed-out write may have sent part
// of a message, so the stream cannot be resumed anyway.
type deadlineWriter struct {
	conn    net.Conn
	timeout time.Duration
	log     Log
}

func (w *deadlineWriter) Write(msg []byte) (int, error) {
	if err := w.conn.SetWriteDeadline(time.Now().Add(w.timeout)); err != nil {
		return 0, err
	}

	n, err := w.conn.Write(msg)
	if err != nil && errors.Is(err, os.ErrDeadlineExceeded) {
		w.log.OnEventf("Socket write timed out after %v, closing connection", w.timeout)
		if closeErr := closeTransport(w.conn); closeErr != nil {
			w.log.OnEvent(closeErr.Error())
		}
	}

	return n, err
}

// closeTransport closes the connection under any TLS layer. tls.Conn.Close would first try to send
// a close_notify alert, which waits up to 5 more seconds on a peer that is not reading.
func closeTransport(conn net.Conn) error {
	if tlsConn, ok := conn.(*tls.Conn); ok {
		return tlsConn.NetConn().Close()
	}

	return conn.Close()
}

// setLingerZero sets SO_LINGER on with a zero timeout on the TCP connection under conn, so closing
// it sends a RST and discards unsent data rather than a FIN queued behind that data.
func setLingerZero(conn net.Conn) error {
	for {
		switch c := conn.(type) {
		case interface{ SetLinger(sec int) error }:
			return c.SetLinger(0)
		case *tls.Conn:
			conn = c.NetConn()
		case interface{ Raw() net.Conn }:
			// A PROXY protocol connection (github.com/pires/go-proxyproto).
			conn = c.Raw()
		default:
			return fmt.Errorf("cannot set linger on a %T connection", conn)
		}
	}
}
