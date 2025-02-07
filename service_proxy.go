package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"like.org/gw/proxy"
)

type ServiceProxy struct {
	sync.RWMutex
	proxy     *proxy.Proxy
	service   string
	path      string
	strip     bool
	instances []model.Instance
}

func (sp *ServiceProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sp.RLock()
	defer sp.RUnlock()
	sp.proxy.ServeHTTP(w, r)
}

func (sp *ServiceProxy) UpdateInstances(instances []model.Instance) {
	sp.Lock()
	defer sp.Unlock()

	sp.instances = instances
	targets := make([]string, len(instances))
	for i, instance := range instances {
		targets[i] = fmt.Sprintf("http://%s:%d", instance.Ip, instance.Port)
	}

	newProxy, err := proxy.NewProxy(targets, sp.strip, sp.path)
	if err != nil {
		log.Printf("Failed to update proxy for %s: %v", sp.service, err)
		return
	}

	sp.proxy = newProxy
	log.Printf("Updated service %s with targets: %v", sp.service, targets)
}
