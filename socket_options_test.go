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
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/quickfixgo/quickfix/config"
)

// failAfter bounds how long a test waits for a state before declaring that it never arrived. It
// only decides a failing run: a passing run proceeds as soon as the state is reached.
const failAfter = 30 * time.Second

// socketTestApp records logon and logout. Every other callback accepts.
type socketTestApp struct {
	logon  chan struct{}
	logout chan struct{}
}

func newSocketTestApp() *socketTestApp {
	return &socketTestApp{logon: make(chan struct{}, 16), logout: make(chan struct{}, 16)}
}

func (a *socketTestApp) OnCreate(SessionID)                               {}
func (a *socketTestApp) OnLogon(SessionID)                                { a.logon <- struct{}{} }
func (a *socketTestApp) OnLogout(SessionID)                               { a.logout <- struct{}{} }
func (a *socketTestApp) ToAdmin(*Message, SessionID)                      {}
func (a *socketTestApp) ToApp(*Message, SessionID) error                  { return nil }
func (a *socketTestApp) FromAdmin(*Message, SessionID) MessageRejectError { return nil }
func (a *socketTestApp) FromApp(*Message, SessionID) MessageRejectError   { return nil }

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(failAfter):
		t.Fatalf("timed out after %v waiting for %s", failAfter, what)
	}
}

// rawMessage builds a FIX.4.2 frame as a counterparty would send it.
func rawMessage(msgType, sender, target string, seqNum int, body map[Tag]string) []byte {
	msg := NewMessage()
	msg.Header.SetField(tagBeginString, FIXString(BeginStringFIX42))
	msg.Header.SetField(tagMsgType, FIXString(msgType))
	msg.Header.SetField(tagSenderCompID, FIXString(sender))
	msg.Header.SetField(tagTargetCompID, FIXString(target))
	msg.Header.SetField(tagMsgSeqNum, FIXInt(seqNum))
	msg.Header.SetField(tagSendingTime, FIXUTCTimestamp{Time: time.Now().UTC(), Precision: Millis})
	for tag, value := range body {
		msg.Body.SetField(tag, FIXString(value))
	}
	return msg.build()
}

func rawLogon(sender, target string, seqNum int) []byte {
	return rawMessage("A", sender, target, seqNum, map[Tag]string{tagEncryptMethod: "0", tagHeartBtInt: "30"})
}

// bigNews is an application message large enough that a few of them fill a socket's buffers.
func bigNews() *Message {
	msg := NewMessage()
	msg.Header.SetField(tagMsgType, FIXString("B"))
	msg.Body.SetField(Tag(148), FIXString("headline"))
	msg.Body.SetField(tagText, FIXString(strings.Repeat("x", 64*1024)))
	return msg
}

// sendUntilLoggedOut sends large messages on sessionID until the session logs out. Each send blocks
// while the socket cannot take it, so the returned channel closes only once every send is released.
func sendUntilLoggedOut(t *testing.T, sessionID SessionID, loggedOut <-chan struct{}) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-loggedOut:
				return
			default:
			}
			if err := SendToTarget(bigNews(), sessionID); err != nil {
				t.Errorf("SendToTarget: %v", err)
				return
			}
		}
	}()
	return done
}

func listenLoopback(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

func initiatorSettings(t *testing.T, ln net.Listener, sender, target string, extra map[string]string) *Settings {
	t.Helper()
	_, port, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)

	settings := NewSettings()
	session := NewSessionSettings()
	session.Set(config.BeginString, BeginStringFIX42)
	session.Set(config.SenderCompID, sender)
	session.Set(config.TargetCompID, target)
	session.Set(config.SocketConnectHost, "127.0.0.1")
	session.Set(config.SocketConnectPort, port)
	session.Set(config.HeartBtInt, "30")
	session.Set(config.ReconnectInterval, "3600")
	session.Set(config.PersistMessages, "N")
	for k, v := range extra {
		session.Set(k, v)
	}
	_, err = settings.AddSession(session)
	require.NoError(t, err)
	return settings
}

// acceptorOn starts an Acceptor serving one FIX.4.2 session on ln.
func acceptorOn(t *testing.T, app Application, ln net.Listener, sender, target string, extra map[string]string) *Acceptor {
	t.Helper()
	_, port, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)

	settings := NewSettings()
	settings.GlobalSettings().Set(config.SocketAcceptPort, port)
	session := NewSessionSettings()
	session.Set(config.BeginString, BeginStringFIX42)
	session.Set(config.SenderCompID, sender)
	session.Set(config.TargetCompID, target)
	session.Set(config.PersistMessages, "N")
	for k, v := range extra {
		session.Set(k, v)
	}
	_, err = settings.AddSession(session)
	require.NoError(t, err)

	acceptor, err := NewAcceptor(app, NewMemoryStoreFactory(), settings, NewNullLogFactory())
	require.NoError(t, err)
	acceptor.SetNewListenerCallback(func(string, *tls.Config) (net.Listener, error) { return ln, nil })
	require.NoError(t, acceptor.Start())
	return acceptor
}

// readUntilError reads conn until it fails and returns the error.
func readUntilError(conn net.Conn) error {
	_, err := io.Copy(io.Discard, conn)
	if err == nil {
		// io.Copy reports a clean EOF as nil.
		return io.EOF
	}
	return err
}

func TestConnWriterWithoutTimeoutIsTheConnection(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	assert.Equal(t, io.Writer(client), connWriter(client, 0, nullLog{}))
}

func TestDeadlineWriterClosesTheConnectionWhenAWriteTimesOut(t *testing.T) {
	cert, err := tls.LoadX509KeyPair("_test_data/localhost.crt", "_test_data/localhost.key")
	require.NoError(t, err)

	for _, useTLS := range []bool{false, true} {
		t.Run("tls="+strconv.FormatBool(useTLS), func(t *testing.T) {
			ln := listenLoopback(t)
			if useTLS {
				ln = tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}})
			}

			// The peer completes the handshake (TLS) and then never reads again.
			accepted := make(chan net.Conn, 1)
			go func() {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				if tlsConn, ok := conn.(*tls.Conn); ok {
					_ = tlsConn.Handshake()
				}
				accepted <- conn
			}()

			var conn net.Conn
			if useTLS {
				conn, err = tls.Dial("tcp", ln.Addr().String(), &tls.Config{InsecureSkipVerify: true}) //nolint:gosec
			} else {
				conn, err = net.Dial("tcp", ln.Addr().String())
			}
			require.NoError(t, err)
			defer conn.Close()
			peer := <-accepted
			defer peer.Close()

			w := connWriter(conn, 100*time.Millisecond, nullLog{})
			chunk := make([]byte, 64*1024)
			for {
				if _, err = w.Write(chunk); err != nil {
					break
				}
			}

			assert.ErrorIs(t, err, os.ErrDeadlineExceeded)
			// Under TLS it is the TCP connection that is closed, so no close_notify is attempted.
			transport := conn
			if tlsConn, ok := conn.(*tls.Conn); ok {
				transport = tlsConn.NetConn()
			}
			_, err = transport.Write(chunk)
			assert.ErrorIs(t, err, net.ErrClosed, "the timed-out write must close the connection")
		})
	}
}

// A peer that stops reading must not wedge the session: the write times out, the connection
// closes, the session disconnects, and every send blocked behind the stalled write returns.
func TestInitiatorSocketWriteTimeoutDisconnectsFromAPeerThatStopsReading(t *testing.T) {
	ln := listenLoopback(t)
	release := make(chan struct{})
	defer close(release)

	// The counterparty answers the Logon and then never reads again.
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := newParser(bufio.NewReader(conn)).ReadMessage(); err != nil {
			return
		}
		if _, err := conn.Write(rawLogon("IWT_ACC", "IWT_INI", 1)); err != nil {
			return
		}
		<-release
	}()

	app := newSocketTestApp()
	settings := initiatorSettings(t, ln, "IWT_INI", "IWT_ACC", map[string]string{config.SocketWriteTimeout: "300ms"})
	initiator, err := NewInitiator(app, NewMemoryStoreFactory(), settings, NewNullLogFactory())
	require.NoError(t, err)
	require.NoError(t, initiator.Start())

	waitFor(t, app.logon, "logon")
	sessionID := SessionID{BeginString: BeginStringFIX42, SenderCompID: "IWT_INI", TargetCompID: "IWT_ACC"}
	loggedOut := make(chan struct{})
	sendsDone := sendUntilLoggedOut(t, sessionID, loggedOut)

	waitFor(t, app.logout, "the session to disconnect after the write timed out")
	close(loggedOut)
	waitFor(t, sendsDone, "the blocked senders to be released")

	// Only stopped on success: a session wedged on a write cannot process a stop request.
	initiator.Stop()
}

func TestAcceptorSocketWriteTimeoutDisconnectsFromAPeerThatStopsReading(t *testing.T) {
	ln := listenLoopback(t)
	app := newSocketTestApp()
	acceptor := acceptorOn(t, app, ln, "AWT_ACC", "AWT_INI", map[string]string{config.SocketWriteTimeout: "300ms"})

	// The counterparty logs on and then never reads.
	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write(rawLogon("AWT_INI", "AWT_ACC", 1))
	require.NoError(t, err)

	waitFor(t, app.logon, "logon")
	sessionID := SessionID{BeginString: BeginStringFIX42, SenderCompID: "AWT_ACC", TargetCompID: "AWT_INI"}
	loggedOut := make(chan struct{})
	sendsDone := sendUntilLoggedOut(t, sessionID, loggedOut)

	waitFor(t, app.logout, "the session to disconnect after the write timed out")
	close(loggedOut)
	waitFor(t, sendsDone, "the blocked senders to be released")

	acceptor.Stop()
}

// With SocketLingerZeroOnClose=Y the counterparty sees the close as a reset; without it, as EOF.
func TestInitiatorSocketLingerZeroOnClose(t *testing.T) {
	tests := []struct {
		setting string
		want    error
	}{
		{"Y", syscall.ECONNRESET},
		{"N", io.EOF},
	}

	for i, tt := range tests {
		t.Run(tt.setting, func(t *testing.T) {
			ln := listenLoopback(t)
			sender, target := "ILZ_INI"+strconv.Itoa(i), "ILZ_ACC"+strconv.Itoa(i)

			// The counterparty never answers the Logon, so the initiator's logon timeout closes the
			// connection.
			peerErr := make(chan error, 1)
			go func() {
				conn, err := ln.Accept()
				if err != nil {
					peerErr <- err
					return
				}
				defer conn.Close()
				peerErr <- readUntilError(conn)
			}()

			settings := initiatorSettings(t, ln, sender, target, map[string]string{
				config.SocketLingerZeroOnClose: tt.setting,
				config.LogonTimeout:            "200ms",
			})
			initiator, err := NewInitiator(newSocketTestApp(), NewMemoryStoreFactory(), settings, NewNullLogFactory())
			require.NoError(t, err)
			require.NoError(t, initiator.Start())
			defer initiator.Stop()

			select {
			case err := <-peerErr:
				assert.ErrorIs(t, err, tt.want)
			case <-time.After(failAfter):
				t.Fatalf("timed out after %v waiting for the initiator to close", failAfter)
			}
		})
	}
}

func TestAcceptorSocketLingerZeroOnClose(t *testing.T) {
	tests := []struct {
		setting string
		want    error
	}{
		{"Y", syscall.ECONNRESET},
		{"N", io.EOF},
	}

	for i, tt := range tests {
		t.Run(tt.setting, func(t *testing.T) {
			ln := listenLoopback(t)
			sender, target := "ALZ_ACC"+strconv.Itoa(i), "ALZ_INI"+strconv.Itoa(i)
			app := newSocketTestApp()
			acceptor := acceptorOn(t, app, ln, sender, target, map[string]string{config.SocketLingerZeroOnClose: tt.setting})
			defer acceptor.Stop()

			conn, err := net.Dial("tcp", ln.Addr().String())
			require.NoError(t, err)
			defer conn.Close()

			// Log on, then log out: the acceptor answers the Logout and closes the connection.
			_, err = conn.Write(rawLogon(target, sender, 1))
			require.NoError(t, err)
			waitFor(t, app.logon, "logon")
			_, err = conn.Write(rawMessage("5", target, sender, 2, nil))
			require.NoError(t, err)

			peerErr := make(chan error, 1)
			go func() { peerErr <- readUntilError(conn) }()
			select {
			case err := <-peerErr:
				assert.ErrorIs(t, err, tt.want)
			case <-time.After(failAfter):
				t.Fatalf("timed out after %v waiting for the acceptor to close", failAfter)
			}
		})
	}
}

func TestSetLingerZeroRefusesAConnectionWithNoTCPUnderneath(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	assert.Error(t, setLingerZero(client))
}

func TestSetLingerZeroReachesTheTCPConnectionUnderTLS(t *testing.T) {
	ln := listenLoopback(t)
	peerErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			peerErr <- err
			return
		}
		defer conn.Close()
		peerErr <- readUntilError(conn)
	}()

	raw, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	// No handshake is needed: the linger is a property of the TCP connection under the TLS one.
	tlsConn := tls.Client(raw, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec
	require.NoError(t, setLingerZero(tlsConn))
	require.NoError(t, raw.Close())

	select {
	case err := <-peerErr:
		assert.True(t, errors.Is(err, syscall.ECONNRESET), "got %v", err)
	case <-time.After(failAfter):
		t.Fatal("timed out waiting for the reset")
	}
}
