package system

import (
	"context"
	"net"
	"strings"
)

var publicDNS = []string{
	"8.8.8.8:53",
	"1.1.1.1:53",
}

func InitResolver() {
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial:     resolverDial,
	}
}

func resolverDial(ctx context.Context, network, address string) (net.Conn, error) {
	var dialer net.Dialer

	if !isDefaultFallback(address) {
		conn, err := dialer.DialContext(ctx, network, address)
		if err == nil {
			return conn, nil
		}
	}

	for _, dns := range publicDNS {
		conn, err := dialer.DialContext(ctx, "udp", dns)
		if err == nil {
			return conn, nil
		}
	}

	return nil, &net.OpError{
		Op:  "dial",
		Net: network,
		Err: &net.DNSError{Err: "all DNS servers failed", IsTemporary: true},
	}
}

func isDefaultFallback(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}

	return host == "127.0.0.1" || strings.HasPrefix(host, "[::1]") || host == "::1"
}
