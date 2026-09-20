package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"example.com/live-seafood-transit/internal/transit"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP 监听地址")
	data := flag.String("data", "data/events.jsonl", "事件日志文件路径")
	flag.Parse()

	store, events, err := transit.OpenStore(*data)
	if err != nil {
		log.Fatalf("打开事件日志失败: %v", err)
	}
	defer store.Close()

	svc := transit.NewService(store, events, time.Now)
	log.Printf("已回放 %d 个事件,在途状态恢复完成;服务监听 %s", len(events), *addr)
	log.Fatal(http.ListenAndServe(*addr, transit.NewHandler(svc)))
}
