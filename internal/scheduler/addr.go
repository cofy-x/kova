package scheduler

import (
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

// Addr represents a single resolved buildkitd endpoint.
type Addr struct {
	Original      string
	Addr          string
	NodeIP        string
	Cooldown      time.Duration
	mu            sync.Mutex
	cooldownUntil time.Time
}

func (a *Addr) IsInCooldown() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return time.Now().Before(a.cooldownUntil)
}

func (a *Addr) SetCooldown() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cooldownUntil = time.Now().Add(a.Cooldown)
}

func ParseAddrs(s string) ([]*Addr, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("--addrs is required")
	}

	parts := strings.Split(s, ",")
	var addrs []*Addr

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		addr := part
		if !strings.Contains(addr, "://") {
			addr = "tcp://" + addr
		}

		hostPort := strings.TrimPrefix(addr, "tcp://")
		host, port, err := net.SplitHostPort(hostPort)
		if err != nil {
			addrs = append(addrs, &Addr{Original: part, Addr: addr, NodeIP: NodeIPFromAddr(addr)})
			continue
		}

		if ip := net.ParseIP(host); ip != nil {
			addrs = append(addrs, &Addr{Original: part, Addr: addr, NodeIP: ip.String()})
			continue
		}

		ips, err := net.LookupHost(host)
		if err != nil {
			addrs = append(addrs, &Addr{Original: part, Addr: addr, NodeIP: NodeIPFromAddr(addr)})
			continue
		}

		for _, ip := range ips {
			resolved := fmt.Sprintf("tcp://%s", net.JoinHostPort(ip, port))
			addrs = append(addrs, &Addr{Original: part, Addr: resolved, NodeIP: ip})
		}
	}

	if len(addrs) == 0 {
		return nil, fmt.Errorf("no valid buildkitd addresses")
	}
	sort.Slice(addrs, func(i, j int) bool {
		return addrs[i].Addr < addrs[j].Addr
	})
	return addrs, nil
}

func NodeIPFromAddr(addr string) string {
	hostPort := strings.TrimPrefix(strings.TrimSpace(addr), "tcp://")
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		return ""
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	return ""
}
