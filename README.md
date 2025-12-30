# Yet Another Simple Paxos

## 实验目标

了解共识的运行过程，并动手使用 Go 实现共识协议，通过协议的运行保证所有参与节点以相同的顺序对所有的请求进行提交。

本项目意在使用 CSP 模型实现一个简单的共识算法，在巩固共识算法理解之余，理解 CSP 模型，了解一些实现惯例和设计模型，领略原始的 Go 味道。

## 快速测试

```bash
$ cd scripts
$ bash ./run_and_check.sh
Check on low pressure
----------begin to check------------
node 0 commits 300 times
node 1 commits 400 times
node 2 commits 500 times
node 3 commits 500 times
node 4 commits 500 times
---------- check validity ------------
pass
---------- check safety --------------
pass
---------- end -----------------------
Check on high pressure
----------begin to check------------
node 0 commits 1521 times
node 1 commits 2056 times
node 2 commits 2520 times
node 3 commits 2521 times
node 4 commits 2520 times
---------- check validity ------------
pass
---------- check safety --------------
pass
---------- end -----------------------
```

## Go

Go 是一个编译型语言，它的特点包括

- 语法极简，没有 "Class"，但有一个易用的现代语言该有的一切（closure, abstract class, async, gc...）
- 契合云原生环境，具有良好的编译速度和跨平台支持，编译产物完全静态链接，极适合在容器中运行
- 从设计之初就全面支持异步，拥有 Garbage Collection，适合高并发网络应用编写（除非要求极限性能）
- 并发模型参考 CSP，拥有“通讯来共享数据”、“避免锁和线程争用”等设计哲学

Go 本身上手很简单，无非变量、函数，一如任何你写过的其他语言；但要真正出稳定高性能的好程序是很难的，需要对异步模型、设计模式等有深刻理解，乃至对运行时有深入研究。

## spec

- cli arguments
  - -i <rank>: rank id
  - -o <timeout>: channel read timeout
  - -l <interval>: max messages a leader can impose
  - -t <background>: background time for test
- log format
  - `generate Instance\[(.*?)\] in seq (\d+) at (\d+)\n`
  - `commit Instance\[(.*?)\] in seq (\d+) at (\d+)\n`
- log validate rule
  1. 单 rank 的 seq id 对应唯一 data
  2. 合并的 commit 集合是合并的 proposal 子集
  3. 任意两个 rank 被 maxSeq 截断的 commit 表 (seq, data) 相等

需要注意的是，rule 3 在现有的算法下是无法实现的，rank 之间可能会出现分叉（假定网络不稳定）；更鲁棒的实现需要像 RAFT 一样实现 AppendEntries。学有余力的同学可以尝试解决。

## 算法

一个共识协议大体要设计数据传递和换主。我们的 simple paxos 算法要求如下：

- 中心化数据传递：中心化广播+2 stage commit 机制保证 quorum。
- 换主：
  - 换主触发：使用固定 timeout 触发换主
    - leader 等待 timeout 消息再启动
    - follower 等待 leader 消息
  - 选举：用 timeout 触发的本地的循环计数器，不需同步
  - 换主同步：read + impose，全量同步

## 设计

我们采用 CSP 架构：有多个单线程的 processor 各自负责一个逻辑模块，类似于老师上课讲的方式，由一个异步调用触发逻辑；不同的是，一段逻辑内部可能会 invoke blocking operation，不然就成纯纯的 callback 模式了，会降低代码可读性。

设计以下 processor：

- msg handler: 负责接收 message 信息，接收 bump 信息，管理 estround, leader, 发给 leader/follower
- consensus handler: 负责接收 propose 和 commit，写入 log
- leader handler: 接收 leader 消息，发送 read, impose, commit
- follower handler: 接收 follower 消息，发送 gather, ack

有一些实现细节的模式约定：

- 没有永远 block 的 operation；所有 channel 读必须要有 timeout
- 所有写 operation 都是 nonblocking
- bump: 有四种原因：round end, timeout, timer trigger, get larger;
  - 重复 bump: 不管了
  - bump 中不可读，会发一个 bumpmsg to active (非 round end)
  - 大概率从 seq=0，小概率 (larger) 从中间开始
- bump: msgr 会发送 bump msg 给 leader or follower (the active one)
- invariant: if leader is active, leader should be reading, and follower should be waiting; vice versa
` 


### 技术选型

## Roadmap

- unit test
- 更好用的版本

