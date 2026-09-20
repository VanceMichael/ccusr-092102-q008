package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"example.com/live-seafood-transit/internal/transit"
)

func main() {
	path := os.Getenv("TRANSIT_DATA")
	if path == "" {
		path = "data/events.jsonl"
	}
	addr := os.Getenv("TRANSIT_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	store, err := transit.Open(path, transit.DefaultThresholds(), time.Now)
	if err != nil {
		log.Fatalf("打开事件存储失败: %v", err)
	}
	defer func() { _ = store.Close() }()
	log.Printf("鲜活水产保税联运服务监听 %s,事件日志 %s", addr, path)
	log.Fatal(http.ListenAndServe(addr, transit.NewHandler(transit.NewService(store))))
}
