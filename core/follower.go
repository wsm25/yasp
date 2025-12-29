package core

import "context"

type Follower struct {
	*Lifetime
	processors   *Processors
	conf         *Config
	followerChan TimeoutChan
}

func NewFollower(conf *Config, processors *Processors) *Follower {
	return &Follower{
		Lifetime:     conf.NewLifetime(),
		processors:   processors,
		conf:         conf,
		followerChan: conf.NewTimeoutChan(),
	}
}

func (f *Follower) Run() {
	defer f.Kill()
	for !f.dead.Load() {
		// new round
		round, seq := f.processors.messager.WaitUntil(false)

		// sync
		if seq == 0 {
			err := f.Sync(round)
			if err != nil {
				if !IsBumpErr(err) {
					f.processors.messager.PushBumpRound(f.ctx)
				}
				continue
			}
		}

		// keep generate and impose
		for s := seq; s <= f.conf.RoundSeqs; s++ {
			err := f.DoSeq(round, s)
			if err != nil {
				if !IsBumpErr(err) {
					f.processors.messager.PushBumpRound(f.ctx)
				}
				continue
			}
			f.processors.messager.BumpSeq(f.ctx)
		}
	}
}

func (f *Follower) Sync(round int) error {
	// 1. read
	var msg *PaxosMsg
	var err error
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
	localData := f.processors.logger.LoadHistory()
	f.processors.messager.Push(f.ctx, &PaxosMsg{
		Type: MsgGather,
		Data: localData,
		To:   msg.From,
	})

	// 3. impose
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
	f.processors.messager.Push(f.ctx, &PaxosMsg{
		Type: MsgAck,
		To:   msg.From,
	})

	// 5. decide
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

func (f *Follower) DoSeq(round int, seq int) error {
	// 1. propose
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
		Estround: round*f.conf.RoundSeqs + seq,
		Data:     data,
	})

	// 2. ack
	f.processors.messager.Push(f.ctx, &PaxosMsg{
		Type: MsgAck,
		To:   msg.From,
	})

	// 3. commit
	for {
		msg, err = f.followerChan.Read(f.ctx)
		if err != nil {
			return err
		}
		if msg.Type == MsgCommit {
			break
		}
	}
	f.processors.logger.Commit(round*f.conf.RoundSeqs + seq)

	return nil
}

func (l *Follower) Push(ctx context.Context, msg *PaxosMsg) {
	l.followerChan.Write(ctx, msg)
}
