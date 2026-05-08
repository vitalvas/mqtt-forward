package socks5

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

const (
	Version5 = 0x05

	AuthNone         = 0x00
	AuthNoAcceptable = 0xFF

	CmdConnect = 0x01

	AddrTypeIPv4   = 0x01
	AddrTypeDomain = 0x03
	AddrTypeIPv6   = 0x04

	ReplySuccess             = 0x00
	ReplyGeneralFailure      = 0x01
	ReplyConnectionRefused   = 0x05
	ReplyCommandNotSupported = 0x07
)

// Handshake performs the SOCKS5 greeting and CONNECT negotiation,
// returning the requested target as "host:port".
func Handshake(conn net.Conn) (string, error) {
	// Read greeting: version + nMethods
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", fmt.Errorf("read greeting: %w", err)
	}

	if header[0] != Version5 {
		return "", fmt.Errorf("unsupported SOCKS version: %d", header[0])
	}

	// Read auth methods
	methods := make([]byte, header[1])
	if _, err := io.ReadFull(conn, methods); err != nil {
		return "", fmt.Errorf("read auth methods: %w", err)
	}

	// Check for no-auth method
	hasNoAuth := false
	for _, m := range methods {
		if m == AuthNone {
			hasNoAuth = true
			break
		}
	}

	if !hasNoAuth {
		conn.Write([]byte{Version5, AuthNoAcceptable})
		return "", errors.New("no acceptable auth method")
	}

	// Accept no-auth
	if _, err := conn.Write([]byte{Version5, AuthNone}); err != nil {
		return "", fmt.Errorf("write auth response: %w", err)
	}

	// Read CONNECT request: version + cmd + reserved + addrType
	req := make([]byte, 4)
	if _, err := io.ReadFull(conn, req); err != nil {
		return "", fmt.Errorf("read request: %w", err)
	}

	if req[0] != Version5 {
		return "", fmt.Errorf("unsupported SOCKS version in request: %d", req[0])
	}

	if req[1] != CmdConnect {
		SendFailure(conn, ReplyCommandNotSupported)
		return "", fmt.Errorf("unsupported command: %d", req[1])
	}

	// Read address
	var host string

	switch req[3] {
	case AddrTypeIPv4:
		addr := make([]byte, 4)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return "", fmt.Errorf("read ipv4 addr: %w", err)
		}

		host = net.IP(addr).String()

	case AddrTypeDomain:
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return "", fmt.Errorf("read domain length: %w", err)
		}

		domain := make([]byte, lenBuf[0])
		if _, err := io.ReadFull(conn, domain); err != nil {
			return "", fmt.Errorf("read domain: %w", err)
		}

		host = string(domain)

	case AddrTypeIPv6:
		addr := make([]byte, 16)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return "", fmt.Errorf("read ipv6 addr: %w", err)
		}

		host = net.IP(addr).String()

	default:
		SendFailure(conn, ReplyGeneralFailure)
		return "", fmt.Errorf("unsupported address type: %d", req[3])
	}

	// Read port (big-endian)
	portBuf := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBuf); err != nil {
		return "", fmt.Errorf("read port: %w", err)
	}

	port := binary.BigEndian.Uint16(portBuf)

	return net.JoinHostPort(host, fmt.Sprintf("%d", port)), nil
}

// SendSuccess writes a successful SOCKS5 CONNECT reply.
func SendSuccess(conn net.Conn, bindAddr net.Addr) error {
	if bindAddr == nil {
		reply := []byte{Version5, ReplySuccess, 0x00, AddrTypeIPv4, 0, 0, 0, 0, 0, 0}
		_, err := conn.Write(reply)

		return err
	}

	tcpAddr, ok := bindAddr.(*net.TCPAddr)
	if !ok {
		reply := []byte{Version5, ReplySuccess, 0x00, AddrTypeIPv4, 0, 0, 0, 0, 0, 0}
		_, err := conn.Write(reply)

		return err
	}

	portBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(portBuf, uint16(tcpAddr.Port))

	if ip4 := tcpAddr.IP.To4(); ip4 != nil {
		reply := make([]byte, 0, 4+len(ip4)+2)
		reply = append(reply, Version5, ReplySuccess, 0x00, AddrTypeIPv4)
		reply = append(reply, ip4...)
		reply = append(reply, portBuf...)
		_, err := conn.Write(reply)

		return err
	}

	ip6 := tcpAddr.IP.To16()
	reply := make([]byte, 0, 4+len(ip6)+2)
	reply = append(reply, Version5, ReplySuccess, 0x00, AddrTypeIPv6)
	reply = append(reply, ip6...)
	reply = append(reply, portBuf...)
	_, err := conn.Write(reply)

	return err
}

// SendFailure writes a failure SOCKS5 CONNECT reply with the given code.
func SendFailure(conn net.Conn, code byte) error {
	reply := []byte{Version5, code, 0x00, AddrTypeIPv4, 0, 0, 0, 0, 0, 0}
	_, err := conn.Write(reply)

	return err
}
