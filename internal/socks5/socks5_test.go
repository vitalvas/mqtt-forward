package socks5

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildGreeting(version byte, methods ...byte) []byte {
	buf := []byte{version, byte(len(methods))}
	buf = append(buf, methods...)

	return buf
}

func buildConnectRequest(addrType byte, addr []byte, port uint16) []byte {
	buf := []byte{Version5, CmdConnect, 0x00, addrType}
	buf = append(buf, addr...)

	portBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(portBuf, port)
	buf = append(buf, portBuf...)

	return buf
}

func TestHandshake(t *testing.T) {
	t.Run("successful_ipv4_connect", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		go func() {
			client.Write(buildGreeting(Version5, AuthNone))

			// Read auth response
			resp := make([]byte, 2)
			client.Read(resp)

			// Send CONNECT to 192.168.1.1:80
			client.Write(buildConnectRequest(AddrTypeIPv4, net.IPv4(192, 168, 1, 1).To4(), 80))
		}()

		target, err := Handshake(server)
		require.NoError(t, err)
		assert.Equal(t, "192.168.1.1:80", target)
	})

	t.Run("successful_domain_connect", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		go func() {
			client.Write(buildGreeting(Version5, AuthNone))

			resp := make([]byte, 2)
			client.Read(resp)

			domain := "example.com"
			addr := append([]byte{byte(len(domain))}, []byte(domain)...)
			client.Write(buildConnectRequest(AddrTypeDomain, addr, 443))
		}()

		target, err := Handshake(server)
		require.NoError(t, err)
		assert.Equal(t, "example.com:443", target)
	})

	t.Run("successful_ipv6_connect", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		go func() {
			client.Write(buildGreeting(Version5, AuthNone))

			resp := make([]byte, 2)
			client.Read(resp)

			ip := net.ParseIP("::1")
			client.Write(buildConnectRequest(AddrTypeIPv6, ip.To16(), 22))
		}()

		target, err := Handshake(server)
		require.NoError(t, err)
		assert.Equal(t, "[::1]:22", target)
	})

	t.Run("invalid_version", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		go func() {
			client.Write(buildGreeting(0x04, AuthNone))
		}()

		_, err := Handshake(server)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported SOCKS version")
	})

	t.Run("no_acceptable_auth", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		go func() {
			client.Write(buildGreeting(Version5, 0x02)) // username/password only

			// Read rejection response
			resp := make([]byte, 2)
			client.Read(resp)
			assert.Equal(t, byte(AuthNoAcceptable), resp[1])
		}()

		_, err := Handshake(server)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no acceptable auth")
	})

	t.Run("unsupported_command", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		go func() {
			client.Write(buildGreeting(Version5, AuthNone))

			resp := make([]byte, 2)
			client.Read(resp)

			// Send only the 4-byte request header with BIND command (0x02)
			// Don't send address/port since Handshake rejects before reading them
			client.Write([]byte{Version5, 0x02, 0x00, AddrTypeIPv4})

			// Read failure reply (SendFailure writes before Handshake returns)
			reply := make([]byte, 10)
			client.Read(reply)
			assert.Equal(t, byte(ReplyCommandNotSupported), reply[1])
		}()

		_, err := Handshake(server)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported command")
	})

	t.Run("short_greeting", func(t *testing.T) {
		client, server := net.Pipe()
		defer server.Close()

		go func() {
			client.Write([]byte{Version5})
			client.Close()
		}()

		_, err := Handshake(server)
		assert.Error(t, err)
	})

	t.Run("short_request", func(t *testing.T) {
		client, server := net.Pipe()
		defer server.Close()

		go func() {
			client.Write(buildGreeting(Version5, AuthNone))

			resp := make([]byte, 2)
			client.Read(resp)

			// Partial request
			client.Write([]byte{Version5, CmdConnect})
			client.Close()
		}()

		_, err := Handshake(server)
		assert.Error(t, err)
	})

	t.Run("unsupported_addr_type", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		go func() {
			client.Write(buildGreeting(Version5, AuthNone))

			resp := make([]byte, 2)
			client.Read(resp)

			// Send only the 4-byte request header with unsupported addr type
			client.Write([]byte{Version5, CmdConnect, 0x00, 0xFF})

			// Read failure reply
			reply := make([]byte, 10)
			client.Read(reply)
		}()

		_, err := Handshake(server)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported address type")
	})
}

func TestSendSuccess(t *testing.T) {
	t.Run("with_nil_addr", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		go func() {
			err := SendSuccess(server, nil)
			assert.NoError(t, err)
		}()

		reply := make([]byte, 10)
		_, err := client.Read(reply)
		require.NoError(t, err)

		assert.Equal(t, byte(Version5), reply[0])
		assert.Equal(t, byte(ReplySuccess), reply[1])
		assert.Equal(t, byte(AddrTypeIPv4), reply[3])
	})

	t.Run("with_tcp_addr", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		addr := &net.TCPAddr{
			IP:   net.IPv4(10, 0, 0, 1),
			Port: 8080,
		}

		go func() {
			err := SendSuccess(server, addr)
			assert.NoError(t, err)
		}()

		reply := make([]byte, 10)
		_, err := client.Read(reply)
		require.NoError(t, err)

		assert.Equal(t, byte(ReplySuccess), reply[1])
		assert.Equal(t, byte(10), reply[4])
		assert.Equal(t, byte(0), reply[5])
		assert.Equal(t, byte(0), reply[6])
		assert.Equal(t, byte(1), reply[7])

		port := binary.BigEndian.Uint16(reply[8:10])
		assert.Equal(t, uint16(8080), port)
	})

	t.Run("with_ipv6_addr", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		addr := &net.TCPAddr{
			IP:   net.ParseIP("::1"),
			Port: 443,
		}

		go func() {
			err := SendSuccess(server, addr)
			assert.NoError(t, err)
		}()

		// IPv6 reply: 4 header + 16 IP + 2 port = 22 bytes
		reply := make([]byte, 22)
		_, err := client.Read(reply)
		require.NoError(t, err)

		assert.Equal(t, byte(Version5), reply[0])
		assert.Equal(t, byte(ReplySuccess), reply[1])
		assert.Equal(t, byte(AddrTypeIPv6), reply[3])

		ip := net.IP(reply[4:20])
		assert.True(t, net.ParseIP("::1").Equal(ip))

		port := binary.BigEndian.Uint16(reply[20:22])
		assert.Equal(t, uint16(443), port)
	})
}

func TestSendFailure(t *testing.T) {
	t.Run("connection_refused", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		go func() {
			err := SendFailure(server, ReplyConnectionRefused)
			assert.NoError(t, err)
		}()

		reply := make([]byte, 10)
		_, err := client.Read(reply)
		require.NoError(t, err)

		assert.Equal(t, byte(Version5), reply[0])
		assert.Equal(t, byte(ReplyConnectionRefused), reply[1])
	})
}
