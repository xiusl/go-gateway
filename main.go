package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"like.org/gw/config"
)

func main() {
	cfg, err := config.LoadConfig("config/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	nacosCli, err := createNacosClient()
	if err != nil {
		log.Fatalf("Failed to create nacos client: %v", err)
	}

	router := mux.NewRouter()

	for _, route := range cfg.Routes {
		serviceProxy := &ServiceProxy{
			service: route.Service,
			path:    route.Path,
			strip:   true,
		}

		err = nacosCli.Subscribe(&vo.SubscribeParam{
			ServiceName: route.Service,
			SubscribeCallback: func(services []model.Instance, err error) {
				log.Printf("Subscribe service: %v", services)
				if err != nil {
					log.Printf("Failed to subscribe service: %v", err)
					return
				}
				instances := make([]model.Instance, 0, len(services))
				for _, service := range services {
					if service.Healthy {
						instances = append(instances, service)
					}
				}
				serviceProxy.UpdateInstances(instances)
			},
		})
		if err != nil {
			log.Fatalf("Failed to subscribe service: %v", err)
		}

		// 从 Nacos 获取服务实例
		instances, _ := nacosCli.SelectInstances(vo.SelectInstancesParam{
			ServiceName: route.Service,
			HealthyOnly: true,
		})
		serviceProxy.UpdateInstances(instances)

		router.PathPrefix(route.Path).Handler(serviceProxy)
	}

	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	srv := &http.Server{
		Addr:    addr,
		Handler: router,
	}

	// 在独立的 goroutine 中启动服务器
	go func() {
		log.Printf("Gateway server starting on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// 创建一个带超时的上下文用于优雅关闭
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	for _, route := range cfg.Routes {
		if err := nacosCli.Unsubscribe(&vo.SubscribeParam{
			ServiceName: route.Service,
		}); err != nil {
			log.Printf("Failed to unsubscribe service: %v", err)
		} else {
			log.Printf("Unsubscribe service %s successfully", route.Service)
		}
	}

	// 关闭 Nacos 客户端
	nacosCli.CloseClient()

	log.Println("Server exiting")
}

func createNacosClient() (naming_client.INamingClient, error) {
	sc := []constant.ServerConfig{
		{
			IpAddr: "localhost",
			Port:   8848,
		},
	}

	cc := constant.ClientConfig{
		NamespaceId:          "like",
		TimeoutMs:            5000,
		NotLoadCacheAtStart:  true,
		LogDir:               "/tmp/nacos/log",
		CacheDir:             "/tmp/nacos/cache",
		LogLevel:             "debug",
		UpdateCacheWhenEmpty: true,
	}

	client, err := clients.CreateNamingClient(map[string]interface{}{
		"serverConfigs": sc,
		"clientConfig":  cc,
	})

	return client, err
}
