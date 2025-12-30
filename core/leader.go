package core

import (
	"context"
	"crypto/rand"
	"slices"
)

type Leader struct {
	*Lifetime
	roundseq   *RoundSeqManager
	processors *Processors
	conf       *Config
	leaderChan TimeoutChan
}

func NewLeader(conf *Config, processors *Processors) *Leader {
	return &Leader{
		Lifetime:   conf.NewLifetime(),
		roundseq:   NewRoundSeq(conf),
		processors: processors,
		conf:       conf,
		leaderChan: conf.NewTimeoutChan(),
	}
}

func (l *Leader) Run() {
	defer l.Kill()
	for !l.dead.Load() {
		// new round
		println("Waiting leader")
		rs := l.roundseq.WaitLeader()
		round, seq := rs.Round, rs.Seq
		println("New leader round", round, seq)

		// sync
		if seq == 0 {
			err := l.Sync(rs)
			if err != nil {
				if !IsBumpErr(err) {
					// l.processors.roundseq.BumpRound(round + 1)
				}
				continue
			}
		}

		// keep generate and impose
		for {
			err := l.DoSeq(rs)
			if err != nil {
				if !IsBumpErr(err) {
					// l.processors.roundseq.BumpRound(round + 1)
				}
				continue
			}
			rs = l.roundseq.BumpSeq()
			seq++
			if rs.Round != round || rs.Seq != seq {
				break
			}
		}
	}
}

// init sync at begin of round
func (l *Leader) Sync(rs RoundSeq) error {
	quorum := l.conf.Q

	// 1. read
	l.processors.messager.Push(l.ctx, &PaxosMsg{
		Type: MsgRead,
		To:   BroadcaseId,
	}, rs)

	// 2. gather
	var ready uint8 = 0
	localData := make([]Log, 0)
	localMap := make(map[int]int) // Estround -> index in localData
	for ready < quorum {
		msg, err := l.leaderChan.Read(l.ctx)
		if err != nil {
			return err
		}
		if msg.Type != MsgGather {
			continue
		}
		data, ok := msg.Data.([]Log) // see logger.go
		if !ok {
			continue
		}
		// merge into localData
		for _, remoteLog := range data {
			if idx, exists := localMap[remoteLog.Estround]; !exists {
				// New log, add it
				localData = append(localData, remoteLog)
				localMap[remoteLog.Estround] = len(localData) - 1
			} else if string(localData[idx].Data) != string(remoteLog.Data) {
				// Conflict: prefer remote (replace)
				localData[idx] = remoteLog
			}
		}
		ready++
	}
	// sort by estround
	slices.SortFunc(localData, func(a Log, b Log) int {
		return a.Estround - b.Estround
	})

	// 3. impose
	l.processors.messager.Push(l.ctx, &PaxosMsg{
		Type: MsgImpose,
		Data: localData,
		To:   BroadcaseId,
	}, rs)

	// 4. ack
	ready = 0
	for ready < quorum {
		msg, err := l.leaderChan.Read(l.ctx)
		if err != nil {
			return err
		}
		if msg.Type == MsgAck {
			ready++
		}
	}

	// 5. decide
	l.processors.messager.Push(l.ctx, &PaxosMsg{
		Type: MsgDecide,
		To:   BroadcaseId,
	}, rs)

	return nil
}

func (l *Leader) DoSeq(rs RoundSeq) error {
	// generate and impose
	data := make([]byte, l.conf.DataSize)
	rand.Read(data)

	// 1. propose
	l.processors.logger.logger.Printf("Leader proposing, round=%d, seq=%d", rs.Round, rs.Seq)
	l.processors.messager.Push(l.ctx, &PaxosMsg{
		Type: MsgPropose,
		Data: data,
		To:   BroadcaseId,
	}, rs)

	// 2. ack
	l.processors.logger.logger.Printf("Leader acking, round=%d, seq=%d", rs.Round, rs.Seq)
	var ready uint8 = 0
	for ready < l.conf.Q {
		msg, err := l.leaderChan.Read(l.ctx)
		if err != nil {
			return err
		}
		if msg.Type == MsgAck {
			ready++
		}
	}

	// 3. commit
	l.processors.logger.logger.Printf("Leader commiting, round=%d, seq=%d", rs.Round, rs.Seq)
	l.processors.messager.Push(l.ctx, &PaxosMsg{
		Type: MsgCommit,
		To:   BroadcaseId,
	}, rs)
	return nil
}

func (l *Leader) Push(ctx context.Context, msg *PaxosMsg) {
	l.leaderChan.Write(ctx, msg)
}
