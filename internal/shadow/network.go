package shadow

import (
	"context"
	"io"
	"net"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"
)

const publicIPTimeout = 1 * time.Second

var publicIPURLs = []string{
	"https://checkip.amazonaws.com/",
	"http://whatismyip.akamai.com/",
}

var publicIPHTTPClient = &http.Client{
	Timeout: publicIPTimeout,
}

func publicIPs(ctx context.Context) []string {
	ctx, cancel := context.WithTimeout(ctx, publicIPTimeout)
	defer cancel()

	var result []string

	for _, url := range publicIPURLs {
		if ip := publicIPHTTP(ctx, url); ip != "" {
			if !slices.Contains(result, ip) {
				result = append(result, ip)
			}

			break
		}
	}

	for _, ip := range publicIPDNS(ctx) {
		if !slices.Contains(result, ip) {
			result = append(result, ip)
		}
	}

	sort.Strings(result)

	return result
}

func publicIPHTTP(ctx context.Context, url string) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ""
	}

	req.Header.Set("User-Agent", "curl/8.7.1")

	resp, err := publicIPHTTPClient.Do(req)
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

func publicIPDNS(ctx context.Context) []string {
	var result []string

	if ip := publicIPDNSOpendns(ctx); ip != "" {
		result = append(result, ip)
	}

	for _, ip := range publicIPDNSGoogle(ctx) {
		if !slices.Contains(result, ip) {
			result = append(result, ip)
		}
	}

	return result
}

func publicIPDNSOpendns(ctx context.Context) string {
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			dialer := net.Dialer{Timeout: publicIPTimeout}
			return dialer.DialContext(ctx, "udp", "resolver1.opendns.com:53")
		},
	}

	addrs, err := resolver.LookupHost(ctx, "myip.opendns.com")
	if err != nil || len(addrs) == 0 {
		return ""
	}

	return addrs[0]
}

func publicIPDNSGoogle(ctx context.Context) []string {
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			dialer := net.Dialer{Timeout: publicIPTimeout}
			return dialer.DialContext(ctx, "udp", "ns1.google.com:53")
		},
	}

	records, err := resolver.LookupTXT(ctx, "o-o.myaddr.l.google.com")
	if err != nil {
		return nil
	}

	var result []string

	for _, r := range records {
		ip := strings.TrimSpace(r)
		if net.ParseIP(ip) != nil {
			result = append(result, ip)
		}
	}

	return result
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
