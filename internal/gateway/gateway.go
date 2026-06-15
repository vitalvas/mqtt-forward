// Package gateway runs the client side for multiple devices over a single MQTT
// connection. It is "client mode, but many devices": each route maps a local
// listen address to a target host:port on a specific device. Routes are grouped
// by device and each device is served by its own client over the shared
// transport. Intended to sit behind a reverse proxy (e.g. nginx) so inbound
// traffic is forwarded to the right device by listener.
package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/vitalvas/gokit/xcmd"
	"github.com/vitalvas/mqtt-forward/internal/client"
	"github.com/vitalvas/mqtt-forward/internal/tunnel"
)

// Route is a single gateway forward: a local listen address tunnelled to a
// target host:port on the named device.
type Route struct {
	Listen string
	Device string
	Target string
}

// Gateway forwards local listeners to targets on multiple devices using one
// client per device over a shared transport.
type Gateway struct {
	transport tunnel.Transport
	logger    *slog.Logger
	routes    []Route

	mu      sync.Mutex
	clients []*client.Client
}

// New validates the routes and builds a gateway. It returns an error if no
// routes are given, a route has an empty field, or two routes share a listen
// address.
func New(transport tunnel.Transport, routes []Route, logger *slog.Logger) (*Gateway, error) {
	if len(routes) == 0 {
		return nil, fmt.Errorf("no routes specified")
	}

	seen := make(map[string]struct{}, len(routes))

	for _, r := range routes {
		switch {
		case r.Listen == "":
			return nil, fmt.Errorf("route has empty listen address")
		case r.Device == "":
			return nil, fmt.Errorf("route %q has empty device", r.Listen)
		case r.Target == "":
			return nil, fmt.Errorf("route %q has empty target", r.Listen)
		}

		if _, ok := seen[r.Listen]; ok {
			return nil, fmt.Errorf("duplicate listen address %q", r.Listen)
		}

		seen[r.Listen] = struct{}{}
	}

	return &Gateway{
		transport: transport,
		logger:    logger,
		routes:    routes,
	}, nil
}

// CloseAllSessions closes every active session across all device clients. Safe
// to call before Run has created the clients.
func (g *Gateway) CloseAllSessions() {
	g.mu.Lock()
	clients := make([]*client.Client, len(g.clients))
	copy(clients, g.clients)
	g.mu.Unlock()

	for _, c := range clients {
		c.CloseAllSessions()
	}
}

// Run starts one client per device and forwards each device's routes. It blocks
// until ctx is cancelled or any client fails to start a listener, in which case
// it cancels the rest and returns the first error (fail-fast).
func (g *Gateway) Run(ctx context.Context) error {
	byDevice := groupByDevice(g.routes)

	group, _ := xcmd.ErrGroup(ctx)

	for deviceID, forwards := range byDevice {
		c := client.New(g.transport, deviceID, g.logger)

		g.mu.Lock()
		g.clients = append(g.clients, c)
		g.mu.Unlock()

		g.logger.Debug("gateway device", "device", deviceID, "routes", len(forwards))

		group.Go(func(ctx context.Context) error {
			if err := c.RunTCPForwards(ctx, forwards); err != nil {
				return fmt.Errorf("device %s: %w", deviceID, err)
			}

			return nil
		})
	}

	return group.Wait()
}

// groupByDevice collects routes into per-device forward lists.
func groupByDevice(routes []Route) map[string][]client.TCPForward {
	byDevice := make(map[string][]client.TCPForward)

	for _, r := range routes {
		byDevice[r.Device] = append(byDevice[r.Device], client.TCPForward{
			Listen: r.Listen,
			Target: r.Target,
		})
	}

	return byDevice
}
