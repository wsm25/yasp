// message processor
package core

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"time"
)

type Messager struct {
	*Lifetime
	*Processors
	conf *Config

	rpcPeers []*rpc.Client
	msgChan  TimeoutChan // channel of PaxosMsg
	round    int
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

		msgChan: conf.NewTimeoutChan(),
	}
	m.RunRpcServer()
	time.Sleep(time.Second)
	return m
}

func (m *Messager) Run() {
	defer m.Kill()
	// round bumper
	m.conf.Wg.Add(1)
	m.ConnPeers()
	for !m.dead.Load() {
		msg, err := m.msgChan.Read(m.ctx)
		if err != nil {
			continue
		}
		m.HandleMsg(msg)
	}
}

func (m *Messager) HandleMsg(msg *PaxosMsg) {
	round := m.round
	seq := m.follower.roundseq.Seq // choose a random good value
	myid := m.conf.Id

	if msg.To != myid { // TX
		m.Send(msg)
	} else {
		// RX
		m.logger.logger.Printf("Recv msg, type=%d, round=%d, seq=%d, from=%d, to=%d",
			msg.Type, msg.Round, msg.Seq, msg.From, msg.To)
		if msg.Round < round {
			m.Send(&PaxosMsg{
				Type:  MsgNack,
				Round: round,
				Seq:   seq,
				To:    msg.From,
			})
		} else {
			switch msg.Type {
			case MsgNack:
				// ignore
			case MsgRead, MsgImpose, MsgDecide, MsgPropose, MsgCommit:
				// from leader
				m.Processors.follower.Push(m.ctx, msg)
			case MsgGather, MsgAck:
				// from follower
				m.Processors.leader.Push(m.ctx, msg)
			}
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
	println("Listening at", m.conf.Port)
	// TODO: graceful shutdown
	go http.Serve(l, nil)
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
	m.logger.logger.Printf("Send msg, type=%d, round=%d, seq=%d, from=%d, to=%d",
		msg.Type, msg.Round, msg.Seq, msg.From, msg.To)
	if msg.To == BroadcaseId {
		for _, peer := range m.conf.BCPeers {
			msg.To = peer
			if err := m.rpcPeers[peer].Call("Messager.OnReceive", msg, &dummyReply); err != nil {
				m.logger.logger.Printf("ERROR sending to peer %d: %v", peer, err)
			}
		}
	} else {
		if err := m.rpcPeers[msg.To].Call("Messager.OnReceive", msg, &dummyReply); err != nil {
			m.logger.logger.Printf("ERROR sending to peer %d: %v", msg.To, err)
		}
	}
}

func (m *Messager) OnReceive(msg *PaxosMsg, reply *struct{}) error {
	m.msgChan.Write(m.ctx, msg)
	return nil
}

// methods allowed to call from external processors

// only type, to, data will be preserved
func (m *Messager) Push(ctx context.Context, msg *PaxosMsg, rs RoundSeq) {
	msg.From = uint8(rs.MyId)
	msg.Round = rs.Round
	msg.Seq = rs.Seq
	m.msgChan.Write(m.ctx, msg)
}
