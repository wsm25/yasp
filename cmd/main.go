package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"yasp/core"
)

func main() {
	//default: 5 nodes
	id := flag.Int("i", 0, "[node id]")
	timeout := flag.Int("o", 10, "[leader change timeout (sec)]")
	leaderInterval := flag.Int("l", 100, "[leader change interval (seq num)]")
	testTime := flag.Int("t", 50, "[test time (sec)]")
	flag.Parse()

	node := GetConfig(*id)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(*testTime))

	bg := core.BackgroundConfig{
		Context:   ctx,
		Wg:        &sync.WaitGroup{},
		ChanCap:   10,
		Timeout:   time.Second * time.Duration(*timeout),
		RoundSeqs: *leaderInterval,
		Q:         node.N - node.F,
	}

	consensus := core.Run(&core.Config{NodeConfig: node, BackgroundConfig: bg})

	// shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigChan:
		fmt.Println("\n接收到中断信号，正在退出...")
	case <-ctx.Done():
		fmt.Println("\n测试时间已到，正在退出...")
	}
	consensus.Stop()
	cancel()
	bg.Wg.Wait()
	fmt.Printf("Node %v finished test\n", *id)
}
