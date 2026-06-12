// Package forward parses SSH-style local port forwarding specifications.
package forward

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Spec describes a single local port forward: a local listen address that is
// tunnelled to a target host:port on the remote device.
type Spec struct {
	// Listen is the local listen address (e.g. "localhost:8080" or
	// "127.0.0.1:8080"). When the spec omits a bind address it defaults to the
	// loopback interface.
	Listen string
	// Target is the remote host:port the device dials (e.g. "localhost:22").
	Target string
}

// Parse parses an SSH-style local forward specification in the form
// "[bind_address:]port:host:hostport". The trailing two fields are always the
// target host and port; everything before them is the optional bind address
// and the local port. When the bind address is omitted, the forward listens on
// the loopback interface.
//
// IPv6 literals must be wrapped in square brackets, matching SSH and
// net.JoinHostPort conventions, so their colons are not mistaken for field
// separators.
//
// Examples:
//
//	"8080:localhost:22"                  -> Listen "localhost:8080", Target "localhost:22"
//	"127.0.0.1:8080:db:5432"             -> Listen "127.0.0.1:8080", Target "db:5432"
//	"0.0.0.0:8080:db:5432"               -> Listen "0.0.0.0:8080", Target "db:5432"
//	"8080:[2001:db8::1]:443"             -> Listen "localhost:8080", Target "[2001:db8::1]:443"
//	"[::1]:8080:[2001:db8::1]:443"       -> Listen "[::1]:8080", Target "[2001:db8::1]:443"
func Parse(s string) (Spec, error) {
	rest, hostPort, err := splitTarget(s)
	if err != nil {
		return Spec{}, err
	}

	listen, err := splitListen(rest)
	if err != nil {
		return Spec{}, err
	}

	return Spec{Listen: listen, Target: hostPort}, nil
}

// ParseAll parses a list of forward specifications and fails on the first
// invalid entry or on a duplicate listen address.
func ParseAll(specs []string) ([]Spec, error) {
	if len(specs) == 0 {
		return nil, fmt.Errorf("no forwards specified")
	}

	result := make([]Spec, 0, len(specs))
	seen := make(map[string]struct{}, len(specs))

	for _, raw := range specs {
		spec, err := Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid forward %q: %w", raw, err)
		}

		if _, ok := seen[spec.Listen]; ok {
			return nil, fmt.Errorf("duplicate listen address %q", spec.Listen)
		}

		seen[spec.Listen] = struct{}{}
		result = append(result, spec)
	}

	return result, nil
}

// splitTarget peels the trailing "host:hostport" off the specification and
// returns the remaining "[bind_address:]port" prefix along with the normalised
// target address.
func splitTarget(s string) (rest, target string, err error) {
	prefix, port, err := splitPort(s, false)
	if err != nil {
		return "", "", fmt.Errorf("target port: %w", err)
	}

	rest, host, err := splitHost(prefix)
	if err != nil {
		return "", "", fmt.Errorf("target host: %w", err)
	}

	if rest == "" {
		return "", "", fmt.Errorf("expected [bind_address:]port:host:hostport")
	}

	// Strip the separating colon between the local port and the target host.
	rest = strings.TrimSuffix(rest, ":")

	return rest, net.JoinHostPort(host, port), nil
}

// defaultBindAddr is the bind address used when a forward omits one. It binds
// the loopback interface (both IPv4 127.0.0.1 and IPv6 ::1) so forwards are not
// exposed to the network by default. To listen on all interfaces, specify an
// explicit bind address such as "0.0.0.0:" or "[::]:".
const defaultBindAddr = "localhost"

// splitListen turns the "[bind_address:]port" prefix into a normalised listen
// address. A missing bind address defaults to the loopback interface.
func splitListen(prefix string) (string, error) {
	// A bare port with no bind address has no separating colon. A bracketed
	// bind address (e.g. "[::1]:8080") always does, so a missing colon
	// unambiguously means "use the default bind address".
	if !strings.Contains(prefix, ":") {
		if err := validatePort(prefix, true); err != nil {
			return "", fmt.Errorf("local port: %w", err)
		}

		return net.JoinHostPort(defaultBindAddr, prefix), nil
	}

	rest, port, err := splitPort(prefix, true)
	if err != nil {
		return "", fmt.Errorf("local port: %w", err)
	}

	bindAddr, err := normalizeHost(rest)
	if err != nil {
		return "", fmt.Errorf("bind address: %w", err)
	}

	if bindAddr == "" {
		bindAddr = defaultBindAddr
	}

	return net.JoinHostPort(bindAddr, port), nil
}

// normalizeHost strips surrounding brackets from a bracketed IPv6 literal and
// returns the bare host. An empty input is returned unchanged (no bind host).
func normalizeHost(s string) (string, error) {
	if s == "" {
		return "", nil
	}

	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		host := s[1 : len(s)-1]
		if host == "" {
			return "", fmt.Errorf("host is empty")
		}

		return host, nil
	}

	if strings.ContainsAny(s, "[]") {
		return "", fmt.Errorf("malformed bracket in %q", s)
	}

	return s, nil
}

// splitPort peels a trailing ":port" off s and validates the port. It returns
// the remaining prefix (without the trailing colon) and the port string.
func splitPort(s string, allowZero bool) (rest, port string, err error) {
	idx := strings.LastIndex(s, ":")
	if idx < 0 {
		return "", "", fmt.Errorf("expected host:port")
	}

	port = s[idx+1:]
	if err := validatePort(port, allowZero); err != nil {
		return "", "", err
	}

	return s[:idx], port, nil
}

// splitHost peels a trailing host field off s. The host may be a bracketed
// IPv6 literal ("[::1]") or a plain token without colons. It returns the
// remaining prefix (including any separating colon) and the unbracketed host.
func splitHost(s string) (rest, host string, err error) {
	if strings.HasSuffix(s, "]") {
		open := strings.LastIndex(s, "[")
		if open < 0 {
			return "", "", fmt.Errorf("unmatched ']' in %q", s)
		}

		host = s[open+1 : len(s)-1]
		if host == "" {
			return "", "", fmt.Errorf("host is empty")
		}

		return s[:open], host, nil
	}

	idx := strings.LastIndex(s, ":")
	host = s[idx+1:]
	if host == "" {
		return "", "", fmt.Errorf("host is empty")
	}

	if strings.Contains(host, "]") {
		return "", "", fmt.Errorf("unexpected ']' in %q", host)
	}

	return s[:idx+1], host, nil
}

// validatePort checks that p is a valid TCP port. allowZero permits port 0,
// which requests an OS-assigned ephemeral port and is only meaningful for a
// local listen address, not a dial target.
func validatePort(p string, allowZero bool) error {
	if p == "" {
		return fmt.Errorf("port is empty")
	}

	n, err := strconv.Atoi(p)
	if err != nil {
		return fmt.Errorf("port %q is not a number", p)
	}

	lowest := 1
	if allowZero {
		lowest = 0
	}

	if n < lowest || n > 65535 {
		return fmt.Errorf("port %d out of range %d-65535", n, lowest)
	}

	return nil
}
