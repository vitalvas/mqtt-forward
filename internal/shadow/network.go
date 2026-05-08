package shadow

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	publicIPTimeout = 5 * time.Second
	checkIPURL      = "https://checkip.amazonaws.com/"
	dnsResolver     = "resolver1.opendns.com:53"
	dnsQuery        = "myip.opendns.com"
)

var checkIPURLOverride = checkIPURL

func publicIP(ctx context.Context) string {
	ctx, cancel := context.WithTimeout(ctx, publicIPTimeout)
	defer cancel()

	ch := make(chan string, 2)

	go func() {
		if ip := publicIPHTTP(ctx); ip != "" {
			ch <- ip
		}
	}()

	go func() {
		if ip := publicIPDNS(ctx); ip != "" {
			ch <- ip
		}
	}()

	select {
	case ip := <-ch:
		return ip
	case <-ctx.Done():
		return ""
	}
}

func publicIPHTTP(ctx context.Context) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, checkIPURLOverride, nil)
	if err != nil {
		return ""
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ""
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64))
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(body))
}

func publicIPDNS(ctx context.Context) string {
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			dialer := net.Dialer{Timeout: publicIPTimeout}
			return dialer.DialContext(ctx, "udp", dnsResolver)
		},
	}

	addrs, err := resolver.LookupHost(ctx, dnsQuery)
	if err != nil || len(addrs) == 0 {
		return ""
	}

	return addrs[0]
}

func localInterfaces() map[string][]string {
	result := make(map[string][]string)

	ifaces, err := net.Interfaces()
	if err != nil {
		return result
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		if iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		var ips []string

		for _, addr := range addrs {
			ips = append(ips, addr.String())
		}

		if len(ips) > 0 {
			result[iface.Name] = ips
		}
	}

	return result
}
