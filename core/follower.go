package core

import (
	"context"
)

type Follower struct {
	*Lifetime
	roundseq     *RoundSeqManager
	processors   *Processors
	conf         *Config
	followerChan TimeoutChan
}

func NewFollower(conf *Config, processors *Processors) *Follower {
	return &Follower{
		Lifetime:     conf.NewLifetime(),
		roundseq:     NewRoundSeq(conf),
		processors:   processors,
		conf:         conf,
		followerChan: conf.NewTimeoutChan(),
	}
}

func (f *Follower) Run() {
	defer f.Kill()
	for !f.dead.Load() {
		// new round
		rs := f.roundseq.Read()
		round, seq := rs.Round, rs.Seq
		println("New follower round", round, seq)

		// sync
		if seq == 0 {
			err := f.Sync(rs)
			if err != nil {
				if !IsBumpErr(err) {
					// f.processors.roundseq.BumpRound(round + 1)
				}
				continue
			}
		}

		// keep generate and impose
		for {
			err := f.DoSeq(rs)
			if err != nil {
				if !IsBumpErr(err) {
					// f.processors.roundseq.BumpRound(round + 1)
				}
				continue
			}
			rs = f.roundseq.BumpSeq()
			seq++
			if rs.Round != round || rs.Seq != seq {
				break
			}
		}
	}
}

func (f *Follower) Sync(rs RoundSeq) error {
	// 1. read
	var msg *PaxosMsg
	var err error
	f.processors.logger.logger.Printf("Follower reading")
	for {
		msg, err = f.followerChan.Read(f.ctx)
		if err != nil {
			return err
		}
		if msg.Type == MsgRead {
			break
		}
	}

	// 2. gather
	f.processors.logger.logger.Printf("Follower gathering")
	localData := f.processors.logger.LoadHistory()
	f.processors.messager.Push(f.ctx, &PaxosMsg{
		Type: MsgGather,
		Data: localData,
		To:   msg.From,
	}, rs)

	// 3. impose
	f.processors.logger.logger.Printf("Follower imposing")
	for {
		msg, err = f.followerChan.Read(f.ctx)
		if err != nil {
			return err
		}
		if msg.Type == MsgImpose {
			break
		}
	}
	data := msg.Data.([]Log)
	proposing := f.processors.logger.Merge(data)

	// 4. ack
	f.processors.logger.logger.Printf("Follower acking")
	f.processors.messager.Push(f.ctx, &PaxosMsg{
		Type: MsgAck,
		To:   msg.From,
	}, rs)

	// 5. decide
	f.processors.logger.logger.Printf("Follower deciding")
	for {
		msg, err = f.followerChan.Read(f.ctx)
		if err != nil {
			return err
		}
		if msg.Type == MsgDecide {
			break
		}
	}
	for _, estround := range proposing {
		f.processors.logger.Commit(estround)
	}

	return nil
}

func (f *Follower) DoSeq(rs RoundSeq) error {
	// 1. propose
	f.processors.logger.logger.Printf("Follower proposing, round=%d, seq=%d", rs.Round, rs.Seq)
	var msg *PaxosMsg
	var err error
	for {
		msg, err = f.followerChan.Read(f.ctx)
		if err != nil {
			return err
		}
		if msg.Type == MsgPropose {
			break
		}
	}
	data := msg.Data.([]byte)
	f.processors.logger.Propose(Log{
		Estround: rs.Estround,
		Data:     data,
	})

	// 2. ack
	f.processors.logger.logger.Printf("Follower acking, round=%d, seq=%d", rs.Round, rs.Seq)
	f.processors.messager.Push(f.ctx, &PaxosMsg{
		Type: MsgAck,
		To:   msg.From,
	}, rs)

	// 3. commit
	f.processors.logger.logger.Printf("Follower commiting, round=%d, seq=%d", rs.Round, rs.Seq)
	for {
		msg, err = f.followerChan.Read(f.ctx)
		if err != nil {
			return err
		}
		if msg.Type == MsgCommit {
			break
		}
	}
	f.processors.logger.Commit(rs.Estround)

	f.processors.logger.logger.Printf("Follower done, round=%d, seq=%d", rs.Round, rs.Seq)
	return nil
}

func (l *Follower) Push(ctx context.Context, msg *PaxosMsg) {
	l.followerChan.Write(ctx, msg)
}
