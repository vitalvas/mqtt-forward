package system

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"time"
)

const resolveTimeout = 5 * time.Second

var publicDNS = []string{
	"8.8.8.8:53",
	"1.1.1.1:53",
}

func BrokerResolver(broker string, logger *slog.Logger) func(ctx context.Context) ([]string, error) {
	return func(ctx context.Context) ([]string, error) {
		u, err := url.Parse(broker)
		if err != nil {
			return []string{broker}, nil
		}

		host := u.Hostname()
		port := u.Port()

		if net.ParseIP(host) != nil {
			return []string{broker}, nil
		}

		addrs, err := resolveHost(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("resolve %s: %w", host, err)
		}

		logger.Debug("broker resolved", "host", host, "addrs", addrs)

		servers := make([]string, 0, len(addrs))
		for _, addr := range addrs {
			resolved := fmt.Sprintf("%s://%s", u.Scheme, net.JoinHostPort(addr, port))
			if u.Path != "" {
				resolved += u.Path
			}

			servers = append(servers, resolved)
		}

		return servers, nil
	}
}

func resolveHost(ctx context.Context, host string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, resolveTimeout)
	defer cancel()

	addrs, err := net.DefaultResolver.LookupHost(ctx, host)
	if err == nil && len(addrs) > 0 {
		return addrs, nil
	}

	for _, dns := range publicDNS {
		resolver := &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, "udp", dns)
			},
		}

		addrs, err = resolver.LookupHost(ctx, host)
		if err == nil && len(addrs) > 0 {
			return addrs, nil
		}
	}

	return nil, err
}
