package core

import (
	"context"
	"crypto/rand"
	"slices"
	"time"
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

	t := time.NewTicker(l.conf.Timeout)
	defer t.Stop()
	round, seq := -1, 0
	startup := true
round:
	for !l.dead.Load() {
		if startup {
			startup = false
		} else {
			select {
			case <-t.C:
			case <-l.ctx.Done():
				break round
			}
		}
		// new round
		l.roundseq.BumpRound(round + 1)
		// wait for leader (using timeouter)
		rs := l.roundseq.Read()
		round, seq = rs.Round, rs.Seq
		l.processors.logger.logger.Printf("Waiting leader round")
		if !rs.IsLeader() {
			continue
		}
		l.processors.logger.logger.Printf("New leader round %d, seq %d", round, seq)

		ctx, cancel := context.WithTimeout(l.ctx, l.conf.Timeout)
		// sync
		if seq == 0 {
			err := l.Sync(ctx, rs)
			if err != nil {
				l.processors.logger.logger.Printf("Leader sync %d fail with error: %s", round, err.Error())
				cancel()
				continue
			}
		}

		// keep generate and impose
		for range l.conf.RoundSeqs - seq {
			err := l.DoSeq(ctx, rs)
			if err != nil {
				l.processors.logger.logger.Printf("Leader doseq (%d,%d) fail with error: %s", round, seq, err.Error())
				break
			}
			rs = l.roundseq.BumpSeq()
			seq++
			if seq == l.conf.RoundSeqs {
				break
			}
			if rs.Round != round || rs.Seq != seq {
				break
			}
		}
		cancel()
	}
}

// init sync at begin of round
func (l *Leader) Sync(ctx context.Context, rs RoundSeq) error {
	quorum := l.conf.Q

	// 1. read
	l.processors.messager.Push(ctx, &PaxosMsg{
		Type: MsgRead,
		To:   BroadcaseId,
	}, rs)

	// 2. gather
	var ready uint8 = 0
	localData := make([]Log, 0)
	localMap := make(map[int]int) // Estround -> index in localData
	for ready < quorum {
		msg, err := l.leaderChan.ReadChecked(ctx, rs)
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
	l.processors.messager.Push(ctx, &PaxosMsg{
		Type: MsgImpose,
		Data: localData,
		To:   BroadcaseId,
	}, rs)

	// 4. ack
	ready = 0
	for ready < quorum {
		msg, err := l.leaderChan.ReadChecked(ctx, rs)
		if err != nil {
			return err
		}
		if msg.Type == MsgAck {
			ready++
		}
	}

	// 5. decide
	l.processors.messager.Push(ctx, &PaxosMsg{
		Type: MsgDecide,
		To:   BroadcaseId,
	}, rs)

	return nil
}

func (l *Leader) DoSeq(ctx context.Context, rs RoundSeq) error {
	// generate and impose
	data := make([]byte, l.conf.DataSize)
	rand.Read(data)

	// 1. propose
	l.processors.logger.logger.Printf("Leader proposing, round=%d, seq=%d", rs.Round, rs.Seq)
	l.processors.messager.Push(ctx, &PaxosMsg{
		Type: MsgPropose,
		Data: data,
		To:   BroadcaseId,
	}, rs)

	// 2. ack
	l.processors.logger.logger.Printf("Leader acking, round=%d, seq=%d", rs.Round, rs.Seq)
	var ready uint8 = 0
	for ready < l.conf.Q {
		msg, err := l.leaderChan.ReadChecked(ctx, rs)
		if err != nil {
			return err
		}
		if msg.Type == MsgAck {
			ready++
		}
	}

	// 3. commit
	l.processors.logger.logger.Printf("Leader commiting, round=%d, seq=%d", rs.Round, rs.Seq)
	l.processors.messager.Push(ctx, &PaxosMsg{
		Type: MsgCommit,
		To:   BroadcaseId,
	}, rs)
	return nil
}

func (l *Leader) Push(ctx context.Context, msg *PaxosMsg) {
	l.leaderChan.Write(ctx, msg)
}
