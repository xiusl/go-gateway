package proxy

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
)

type Proxy struct {
	targets []*url.URL
	current uint64

	strip bool
	path  string
}

func NewProxy(targets []string, strip bool, path string) (*Proxy, error) {
	var urls []*url.URL
	for _, target := range targets {
		url, err := url.Parse(target)
		if err != nil {
			return nil, err
		}
		urls = append(urls, url)
	}

	return &Proxy{
		targets: urls,
		strip:   strip,
		path:    path,
	}, nil
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	log.Printf("url %s", r.URL)
	if len(p.targets) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"code":    http.StatusServiceUnavailable,
			"message": "No targets available",
			"data":    nil,
		})
		return
	}

	target := p.nextTarget()
	proxy := httputil.NewSingleHostReverseProxy(target)
	r.URL.Host = target.Host
	r.URL.Scheme = target.Scheme
	r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))

	// 在 proxy.ServeHTTP 方法中处理路径
	if p.strip {
		r.URL.Path = strings.TrimPrefix(r.URL.Path, p.path)
		if !strings.HasPrefix(r.URL.Path, "/") {
			r.URL.Path = "/" + r.URL.Path
		}
	}
	fmt.Printf("Request URL: %s\n", r.URL.String())

	proxy.ServeHTTP(w, r)
}

func (p *Proxy) nextTarget() *url.URL {
	current := atomic.AddUint64(&p.current, 1)
	return p.targets[current%uint64(len(p.targets))]
}
