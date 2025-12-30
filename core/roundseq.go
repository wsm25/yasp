package core

import "sync"

// 管理 round, seq
type RoundSeq struct {
	Round     int
	Seq       int
	Estround  int
	MyId      int
	N         int
	Q         int
	Leader    int
	RoundSeqs int
}

type RoundSeqManager struct {
	RoundSeq
	mu       sync.Mutex // must: bump is a stop-the-world operation
	waitCond *sync.Cond
}

func NewRoundSeq(conf *Config) *RoundSeqManager {
	m := new(RoundSeqManager)
	m.RoundSeq = RoundSeq{
		Round:    0,
		Seq:      0,
		Estround: 0,
		Leader:   0,

		// immutable
		MyId:      int(conf.Id),
		N:         int(conf.N),
		Q:         int(conf.N - conf.F),
		RoundSeqs: conf.RoundSeqs,
	}
	m.waitCond = sync.NewCond(&m.mu)
	return m
}

func (r *RoundSeq) IsLeader() bool {
	return r.Leader == r.MyId
}
func (r *RoundSeq) Calc(round, seq int) int {
	return seq + round*r.RoundSeqs
}

func (r *RoundSeq) Recalc() {
	r.Estround = r.Calc(r.Round, r.Seq)
	r.Leader = r.Round % r.N
}

func (r *RoundSeqManager) Read() RoundSeq {
	r.mu.Lock()
	rs := r.RoundSeq
	r.mu.Unlock()
	return rs
}

func (r *RoundSeqManager) BumpSeq() RoundSeq {
	r.mu.Lock()
	r.Seq += 1
	r.Estround += 1
	if r.Seq >= r.RoundSeqs {
		r.Seq = 0
		r.bumpRound(r.Round + 1)
	}
	rs := r.RoundSeq
	r.mu.Unlock()
	return rs
}

func (r *RoundSeqManager) BumpRound(new int) RoundSeq {
	r.mu.Lock()
	r.bumpRound(new)
	rs := r.RoundSeq
	r.mu.Unlock()
	return rs
}

func (r *RoundSeqManager) SetNewer(round int, seq int) RoundSeq {
	r.mu.Lock()
	defer r.mu.Unlock()
	esround := r.Calc(round, seq)
	if esround < r.Estround {
		return r.RoundSeq
	}
	r.Round = round
	r.Seq = seq
	r.Recalc()
	r.waitCond.Broadcast()
	return r.RoundSeq
}

// without lock
func (r *RoundSeqManager) bumpRound(new int) {
	if new <= r.Round {
		return
	}
	r.Round = new
	r.Seq = 0
	r.Recalc()
	r.waitCond.Broadcast()
}

func (r *RoundSeqManager) WaitLeader() RoundSeq {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		if r.IsLeader() {
			return r.RoundSeq
		}
		r.waitCond.Wait()
	}
}
