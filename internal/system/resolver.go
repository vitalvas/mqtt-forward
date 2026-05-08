package system

import (
	"context"
	"net"
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

	conn, err := dialer.DialContext(ctx, network, address)
	if err == nil {
		return conn, nil
	}

	for _, dns := range publicDNS {
		conn, fallbackErr := dialer.DialContext(ctx, "udp", dns)
		if fallbackErr == nil {
			return conn, nil
		}
	}

	return nil, err
}
