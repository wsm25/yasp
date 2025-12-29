package core

import (
	"context"
	"crypto/rand"
	"slices"
)

type Leader struct {
	*Lifetime
	processors *Processors
	conf       *Config
	leaderChan TimeoutChan
}

func NewLeader(conf *Config, processors *Processors) *Leader {
	return &Leader{
		Lifetime:   conf.NewLifetime(),
		processors: processors,
		conf:       conf,
		leaderChan: conf.NewTimeoutChan(),
	}
}

func (l *Leader) Run() {
	defer l.Kill()
	for !l.dead.Load() {
		// new round
		round, seq := l.processors.messager.WaitUntil(false)

		// sync
		if seq == 0 {
			err := l.Sync(round)
			if err != nil {
				if !IsBumpErr(err) {
					l.processors.messager.PushBumpRound(l.ctx)
				}
				continue
			}
		}

		// keep generate and impose
		for s := seq; s <= l.conf.RoundSeqs; s++ {
			err := l.DoSeq(round, s)
			if err != nil {
				if !IsBumpErr(err) {
					l.processors.messager.PushBumpRound(l.ctx)
				}
				continue
			}
			l.processors.messager.BumpSeq(l.ctx)
		}
	}
}

// init sync at begin of round
func (l *Leader) Sync(round int) error {
	quorum := l.conf.Q

	// 1. read
	l.processors.messager.Push(l.ctx, &PaxosMsg{
		Type: MsgRead,
		To:   BroadcaseId,
	})

	// 2. gather
	var ready uint8 = 0
	localData := l.processors.logger.LoadHistory()
	localMap := make(map[int]int) // Estround -> index in localData
	for i, log := range localData {
		localMap[log.Estround] = i
	}
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
	proposing := l.processors.logger.Merge(localData)
	l.processors.messager.Push(l.ctx, &PaxosMsg{
		Type: MsgImpose,
		Data: localData,
		To:   BroadcaseId,
	})

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
	for _, estround := range proposing {
		l.processors.logger.Commit(estround)
	}
	l.processors.messager.Push(l.ctx, &PaxosMsg{
		Type: MsgDecide,
		To:   BroadcaseId,
	})

	return nil
}

func (l *Leader) DoSeq(round int, seq int) error {
	// generate and impose
	data := make([]byte, l.conf.DataSize)
	rand.Read(data)
	estround := round*l.conf.RoundSeqs + seq

	// 1. propose
	l.processors.logger.Propose(Log{
		Estround: estround,
		Data:     data,
	})
	l.processors.messager.Push(l.ctx, &PaxosMsg{
		Type: MsgPropose,
		Data: data,
		To:   BroadcaseId,
	})

	// 2. ack
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
	l.processors.logger.Commit(estround)
	l.processors.messager.Push(l.ctx, &PaxosMsg{
		Type: MsgCommit,
		To:   BroadcaseId,
	})
	return nil
}

func (l *Leader) Push(ctx context.Context, msg *PaxosMsg) {
	l.leaderChan.Write(ctx, msg)

}
