// package core: simple paxos 核心逻辑
package core

type Processors struct {
	logger   *Logger
	follower *Follower
	leader   *Leader
	messager *Messager
}

type Consensus struct {
	*Processors
	*Config
}

// run consensus in background
func Run(config *Config) *Consensus {
	p := new(Processors)

	p.logger = NewLogger(config)
	p.leader = NewLeader(config, p)
	p.follower = NewFollower(config, p)
	p.messager = NewMessager(config, p)

	go p.messager.Run()
	go p.leader.Run()
	go p.follower.Run()
	return &Consensus{Processors: p, Config: config}
}

func (c *Consensus) Stop() {
	c.leader.Kill()
	c.follower.Kill()
	c.messager.Kill()
}
