// message processor
package core

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

type Messager struct {
	*Lifetime
	*Processors
	conf     *Config
	round    int
	estround int
	seq      int
	leaderId uint8

	myId       uint8
	rpcPeers   []*rpc.Client
	leaderCond sync.Cond
	msgChan    TimeoutChan // channel of PaxosMsg
}

const BroadcaseId uint8 = 255

// Message types for processor communication
type PaxosMsgType int

const (
	MsgRead PaxosMsgType = iota
	MsgGather
	MsgImpose
	MsgAck
	MsgDecide
	MsgPropose // in a round
	MsgCommit
	MsgNack // estround < self.round; data: nil
	MsgBump // internally used, to notify leader and follower that round bump happens; data: bool for stable
)

type PaxosMsg struct {
	Type  PaxosMsgType
	From  uint8
	To    uint8
	Data  any
	Seq   int
	Round int
}

func NewMessager(conf *Config, processors *Processors) *Messager {
	m := &Messager{
		Lifetime:   conf.NewLifetime(),
		Processors: processors,
		conf:       conf,
		round:      0,
		seq:        0,
		estround:   0,

		myId:     conf.Id,
		leaderId: 0,

		msgChan: conf.NewTimeoutChan(),
	}
	go m.RunRpcServer()
	return m
}

func (m *Messager) Run() {
	defer m.Kill()
	// round bumper
	m.conf.Wg.Add(1)
	go m.RunRoundBumper()
	m.ConnPeers()
	for !m.dead.Load() {
		msg, err := m.msgChan.Read(m.ctx)
		if err != nil {
			if IsBumpErr(err) {
				m.BumpRound(0)
			}
			continue
		}
		m.HandleMsg(msg)
	}
}

func (m *Messager) HandleMsg(msg *PaxosMsg) {
	if msg.From == m.myId { // TX
		m.Send(msg)
	} else {
		// RX
		estround := m.conf.RoundSeqs*msg.Round + msg.Seq
		if estround < m.estround {
			m.Send(&PaxosMsg{
				Type:  MsgNack,
				Round: m.round,
				Seq:   m.seq,
				To:    msg.From,
			})
		} else if estround > m.estround {
			m.seq = msg.Seq
			if m.round < msg.Round {
				m.BumpRound(msg.Round)
			}
		} else {
			isLeader := m.myId == m.leaderId
			switch msg.Type {
			case MsgNack:
				// ignore
			case MsgRead, MsgImpose, MsgDecide, MsgPropose, MsgCommit:
				// from leader
				if !isLeader {
					m.Processors.follower.Push(m.ctx, msg)
				}
			case MsgGather, MsgAck:
				// from follower
				if isLeader {
					m.Processors.leader.Push(m.ctx, msg)
				}
			}
		}
	}
}

func (m *Messager) RunRoundBumper() {
	defer m.conf.Wg.Done()
	t := time.NewTicker(m.conf.Timeout)
	defer t.Stop()
	for !m.dead.Load() {
		select {
		case <-t.C:
			m.PushBumpRound(m.ctx)
		case <-m.ctx.Done():
		}
	}
}

func (m *Messager) RunRpcServer() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+strconv.Itoa(int(m.conf.Port)))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// TODO: graceful shutdown
	http.Serve(l, nil)
}

func (m *Messager) ConnPeers() {
	for _, port := range m.conf.Committee {
		c, err := rpc.DialHTTP("tcp", "127.0.0.1:"+strconv.Itoa(int(port)))
		if err != nil {
			panic(err)
		}
		m.rpcPeers = append(m.rpcPeers, c)
	}
}

func (m *Messager) Send(msg *PaxosMsg) {
	var dummyReply struct{}
	if msg.To == BroadcaseId {
		for peer := range m.conf.BCPeers {
			msg.To = uint8(peer)
			m.Send(msg)
			m.rpcPeers[peer].Call("Messager.OnReceive", msg, &dummyReply)
		}
	} else {
		m.rpcPeers[msg.To].Call("Messager.OnReceive", msg, &dummyReply)
	}
}

func (m *Messager) OnReceive(msg *PaxosMsg, reply any) error {
	m.msgChan.Write(m.ctx, msg)
	return nil
}

func (m *Messager) BumpRound(new int) {
	if new <= m.round {
		new = m.round + 1
	}
	isLeader := (m.leaderId == m.myId)
	m.round += 1
	m.leaderId = uint8(m.round) % m.conf.N
	m.seq = 0
	m.CalcEstround()

	// notify leader and follower to reset
	msg := &PaxosMsg{
		Type:  MsgBump,
		From:  m.myId,
		To:    m.myId,
		Seq:   m.seq,
		Round: m.round,
	}
	if isLeader {
		m.Processors.leader.Push(m.ctx, msg)
	} else {
		m.Processors.follower.Push(m.ctx, msg)
	}
	// wake up another
	m.leaderCond.Broadcast()
}

func (m *Messager) CalcEstround() {
	m.estround = m.round*m.conf.RoundSeqs + m.seq
}

// methods allowed to call from external processors

// returns round and seq
func (m *Messager) WaitUntil(leader bool) (int, int) {
	for !m.dead.Load() && (m.leaderId == m.myId) != leader {
		m.leaderCond.Wait()
	}
	return m.round, m.seq
}

// only type, to, data will be preserved
func (m *Messager) Push(ctx context.Context, msg *PaxosMsg) {
	msg.From = m.myId
	msg.Seq = m.seq
	msg.Round = m.round
	m.msgChan.Write(m.ctx, msg)
}

// round bump happens when round end or
func (m *Messager) PushBumpRound(ctx context.Context) {
	m.Push(ctx, &PaxosMsg{
		Type: MsgBump,
		To:   m.myId,
	})
}

// should only called by active one of leader and follower
func (m *Messager) BumpSeq(ctx context.Context) {
	m.seq += 1
	if m.seq >= m.conf.RoundSeqs {
		m.seq = 0
		m.PushBumpRound(ctx)
	}
}
