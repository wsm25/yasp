// base: 提供基础工具
package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	NodeConfig
	BackgroundConfig
}

type NodeConfig struct {
	Id        uint8    `json:"id"`
	N         uint8    `json:"n"` // total node number
	F         uint8    `json:"f"` // allowed fail node number
	Port      uint64   `json:"port"`
	Committee []uint64 `json:"committee"`
	DataSize  uint64   `json:"datasize"`
	BCPeers   []uint8  `json:"bcpeers"`
}

type BackgroundConfig struct {
	Context   context.Context
	Wg        *sync.WaitGroup
	ChanCap   int
	Timeout   time.Duration // also leader change timeout
	RoundSeqs int           // allowed sequence per round
	Q         uint8         // quorum
}

var (
	ErrChannelTimeout = errors.New("channel operation timeout")
	ErrNewRound       = errors.New("bump to new round")
	ErrUnexpectedType = errors.New("unexcepted message")
)

func (c *BackgroundConfig) NewLifetime() *Lifetime {
	ctx, cancel := context.WithCancel(c.Context)
	c.Wg.Add(1)
	return &Lifetime{
		ctx, cancel, c.Wg, atomic.Bool{},
	}
}

// 负责管理 handler 生命周期
type Lifetime struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup
	dead   atomic.Bool
}

// if already killed return false
func (t *Lifetime) Kill() bool {
	if t.dead.Swap(true) {
		return false
	}
	t.wg.Done()
	t.cancel()
	return true
}

func (c *BackgroundConfig) NewTimeoutChan() TimeoutChan {
	return TimeoutChan{
		C:       make(chan *PaxosMsg, c.ChanCap),
		timeout: c.Timeout,
	}
}

// 一个有超时时间的 channel
type TimeoutChan struct {
	C       chan *PaxosMsg
	timeout time.Duration
}

// returns false on timeout or close; note new round also treat as error
func (t *TimeoutChan) ReadChecked(ctx context.Context, rs RoundSeq) (*PaxosMsg, error) {
	msg, err := t.Read(ctx)
	if err != nil {
		return msg, err
	}
	if msg.Round != rs.Round || msg.Seq != rs.Seq {
		return msg, ErrNewRound
	}
	return msg, nil
}

func (t *TimeoutChan) Read(ctx context.Context) (*PaxosMsg, error) {
	timer := time.NewTimer(t.timeout)
	defer timer.Stop()
	select {
	case msg := <-t.C:
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, ErrChannelTimeout
	}
}

// returns false on timeout, panic on close (should never happen)
func (t *TimeoutChan) Write(ctx context.Context, v *PaxosMsg) error {
	timer := time.NewTimer(t.timeout)
	defer timer.Stop()

	select {
	case t.C <- v:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ErrChannelTimeout
	}
}

func (t *TimeoutChan) Close() {
	close(t.C)
}

func IsBumpErr(err error) bool {
	return errors.Is(err, ErrNewRound)
}
