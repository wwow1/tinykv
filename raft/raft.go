// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes             map[uint64]bool
	electionRejectNum uint64

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeoutBaseline int
	electionTimeout         int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raft := &Raft{}
	raft.id = c.ID
	raft.State = StateFollower
	raft.heartbeatTimeout = c.HeartbeatTick
	raft.electionTimeoutBaseline = c.ElectionTick
	raft.electionTimeout = rand.Intn(raft.electionTimeoutBaseline) +
		raft.electionTimeoutBaseline
	raft.RaftLog = newLog(c.Storage)
	raft.votes = make(map[uint64]bool)
	raft.Prs = make(map[uint64]*Progress)
	hardState, confState, _ := c.Storage.InitialState()
	if len(c.peers) == 0 {
		c.peers = confState.Nodes
	}
	// maxPeer := c.ID
	for _, peer := range c.peers {
		raft.votes[peer] = false
		raft.Prs[peer] = &Progress{}
		// maxPeer = max(maxPeer, peer)
	}
	raft.Term = hardState.Term
	raft.Vote = hardState.Vote

	_, err := raft.RaftLog.storage.LastIndex()
	if err != nil {
		panic("xxx")
	}
	log.Infof("peer %v start, Term %v, committed = %v, lastIndex = %v, stabled = %v, applied = %v", raft.id, raft.Term,
		raft.RaftLog.committed, raft.RaftLog.LastIndex(), raft.RaftLog.stabled, raft.RaftLog.applied)
	/* 和Lab2的单测冲突
	if maxPeer == raft.id {
		raft.Step(pb.Message{
			MsgType: pb.MessageType_MsgTimeoutNow,
			From:    raft.id,
			To:      raft.id,
			Term:    raft.Term})
	}
	*/
	return raft
}

func (r *Raft) sendSnapshot(to uint64) bool {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		if err == ErrSnapshotTemporarilyUnavailable {
			// 第一次调用Snapshot的时候后台异步生成快照，大概要等到下一轮快照才生成完毕
			return true
		} else {
			panic("Raft::sendSnapshot Snapshot")
		}
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	})
	r.Prs[to].Next = snapshot.Metadata.Index + 1
	// snapshot没有reply msg,所以在这里就先推进Next(Next有偏差不影响正确性)
	return true
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	if prevLogIndex > r.RaftLog.LastIndex() {
		log.Infof("Raft::sendAppend prevLogIndex > lastIndex")
		r.Prs[to].Next = r.RaftLog.LastIndex() + 1
		return r.sendAppend(to)
	}
	if prevLogIndex < r.RaftLog.truncateIndex {
		// 发送快照
		return r.sendSnapshot(to)
	}
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		panic("Raft::sendAppend Term")
	}
	aftEnts := r.RaftLog.entsAfter(prevLogIndex)
	// log.Debug("Term %v, leader %v send AppendRPC to follower %v, prevLogIndex %v, prevLogTerm %v, ents %v", r.Term, r.id, to,
	//	prevLogIndex, prevLogTerm, aftEnts)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Index:   prevLogIndex,
		LogTerm: prevLogTerm,
		Entries: aftEnts,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  util.RaftInvalidIndex,
	})
}

func (r *Raft) sendVoteRequest(to uint64) {
	// send request vote RPC to other raft node
	LastLogIndex := r.RaftLog.LastIndex()
	LastLogTerm, _ := r.RaftLog.Term(LastLogIndex)
	// log.Debug("Term %v, candidate %v send voteRequest to peer %v", r.Term, r.id, to)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   LastLogIndex, // LastLogIndex
		LogTerm: LastLogTerm,  // LastLogTerm
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// i'm leader
	if r.State == StateLeader {
		// increment heartbeatElapsed, and if heartbeatElapsed == heartbeatTimeout
		// send heartbeat RPC to followers
		r.heartbeatElapsed++
		if r.heartbeatElapsed == r.heartbeatTimeout {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
			r.heartbeatElapsed = 0
		}
	} else {
		// i'm candidate or follower
		r.electionElapsed++
		if r.electionElapsed == r.electionTimeout {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			r.electionTimeout = rand.Intn(r.electionTimeoutBaseline) +
				r.electionTimeoutBaseline
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = lead
	r.leadTransferee = None
	// 重置Next和Match
	for _, progress := range r.Prs {
		progress.Next = 0
		progress.Match = 0
	}
	log.Infof("term %v, peer %v become follower", r.Term, r.id)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// update node state
	r.State = StateCandidate
	// increment term
	r.Term++
	// vote for myself
	r.Vote = r.id
	for peer, _ := range r.Prs {
		r.votes[peer] = false
	}
	r.votes[r.id] = true
	// reset timer
	r.electionElapsed = 0
	r.leadTransferee = None
	// log.Debug("Term %v, peer %v become candidate, and start a leaderElection", r.Term, r.id)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	for _, progress := range r.Prs {
		progress.Next = r.RaftLog.LastIndex() + 1
		progress.Match = 0
	}
	log.Infof("term %d, node %d become leader", r.Term, r.id)
	// propose a noop entry
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{}},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleMsgHup(m)
	case pb.MessageType_MsgBeat:
		r.handleMsgBeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow(m)
	}
	return nil
}

func (r *Raft) updateCommitIndex() {
	var matchArr uint64Slice
	for _, pr := range r.Prs {
		matchArr = append(matchArr, pr.Match)
	}
	if len(matchArr) == 0 {
		return
	}
	sort.Sort(matchArr)
	olderCommitted := r.RaftLog.committed
	for wantCommitted := matchArr[(len(matchArr)-1)/2]; wantCommitted > 0; wantCommitted-- {
		// 只允许提交当前周期的entry
		logTerm, _ := r.RaftLog.Term(wantCommitted)
		// 由于deleteNode会删除节点后再调用updateCommitIndex,可能会诱发committed回退的case
		if logTerm == r.Term && wantCommitted > olderCommitted {
			r.RaftLog.committed = wantCommitted
			break
		}
	}
	if olderCommitted < r.RaftLog.committed {
		// committed推进后，需要向follower广播，推进follower.committed
		for peer, _ := range r.Prs {
			if peer == r.id {
				continue
			}
			r.sendAppend(peer)
		}
	}
}

func (r *Raft) appendEntries2Follower(m pb.Message, reply *pb.Message) {
	if !r.CheckTerm(m.Term, m.From) {
		return
	}
	r.Lead = m.From
	r.electionElapsed = 0
	if m.Index > r.RaftLog.LastIndex() {
		// Log长度不到PrevLogIndex
		reply.Index = r.RaftLog.LastIndex() + 1
		reply.LogTerm = None
		return
	}
	if m.Index >= r.RaftLog.truncateIndex {
		// 小于truncateIndex的都是已经apply的日志,一定已经满足一致性
		prevLogTerm, err := r.RaftLog.Term(m.Index)
		if err != nil {
			panic("Raft::appendEntries2Follower Term1")
		}
		if prevLogTerm != m.LogTerm {
			//PrevLogIndex处的日志项的周期号不匹配
			reply.LogTerm = prevLogTerm // conflictTerm
			r.RaftLog.stabled = min(r.RaftLog.stabled, m.Index-1)
			if m.Index == 0 {
				reply.Index = 1
			} else {
				// 寻找合适的回退点
				reply.Index = r.RaftLog.truncateIndex - 1
				for i := m.Index - 1; i >= r.RaftLog.truncateIndex; i-- {
					myLogTerm, err := r.RaftLog.Term(i)
					if err != nil {
						panic("Raft::appendEntries2Follower Term2")
					}
					if myLogTerm < reply.LogTerm {
						reply.Index = i + 1 // conflictIndex
						break
					}
				}
			}
			return
		}
	}
	leaderLastLogIndex := m.Index + uint64(len(m.Entries))
	startIndex := max(m.Index+1, r.RaftLog.truncateIndex+1)
	// startIndex是针对快照场景下的错误处理
	// (appendEntriesRPC过迟到来，m.Entries中包含某些已经被follower截断的日志,在针对这些日志取raftLog.Term的时候会出错,需要跨过它们)
	for i := startIndex; i <= r.RaftLog.LastIndex() && i <= leaderLastLogIndex; i++ {
		//找到不匹配的日志项，删除在它之后的所有日志项
		myLogTerm, err := r.RaftLog.Term(i)
		if err != nil {
			panic("Raft::appendEntries2Follower Term3")
		}
		if myLogTerm != m.Entries[i-startIndex].Term {
			r.RaftLog.stabled = min(r.RaftLog.stabled, i-1) // 产生冲突 TODO(zhengfuyu): maybe error
			r.RaftLog.ClearEntsAfter(i - 1)
			break
		}
	}
	if r.RaftLog.LastIndex() < leaderLastLogIndex {
		// 将新的日志项追加到日志中
		r.RaftLog.AppendEntries(m.Entries[r.RaftLog.LastIndex()-m.Index:], r.Term)
	}
	// 更新commitIndex
	consistentLogIndex := m.Index
	if len(m.Entries) != 0 {
		consistentLogIndex = m.Entries[len(m.Entries)-1].Index
	}
	if consistentLogIndex > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, consistentLogIndex)
	}
	reply.Index = r.RaftLog.LastIndex() // NextIndex
	reply.Reject = false
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	reply := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		Reject:  true,
		From:    r.id,
		To:      m.From,
	}
	r.appendEntries2Follower(m, &reply)
	// log.Debug("Term %v, follower %v reply AppenRPC to leader %v, reject %v, conflictIndex %v, appendEnts %v", r.Term, r.id, m.From,
	//  	reply.Reject, reply.Index, m.Entries)
	reply.Term = r.Term
	r.msgs = append(r.msgs, reply)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if r.State != StateLeader {
		return
	}
	if m.Reject {
		// m.Index = 0也意味着对方的Term比本次rpc发送时携带的Term大
		//(但是rpc返回的时候可能本节点的Term也变大了，导致错误判断并回退index)
		if m.Term > r.Term || m.Index == 0 {
			r.becomeFollower(m.Term, None)
			log.DPrintf("leader %v failed to send appendEntryiesRPC to %v, its term %v is higher than leader's term %v",
				r.id, m.From, m.Term, r.Term)
		} else {
			r.Prs[m.From].Next = m.Index
			r.sendAppend(m.From)
			log.DPrintf("leader %v failed to send appendEntryiesRPC to %v, quickGoBackIndex to %v",
				r.id, m.From, r.Prs[m.From].Next)
		}
		return
	}
	if r.Term != m.Term {
		// 忽略过期的RPC应答
		return
	}
	if m.Index > r.RaftLog.LastIndex()+1 {
		panic("Raft::Step reply.Index > myLastIndex")
	}
	r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
	r.Prs[m.From].Next = max(r.Prs[m.From].Next, m.Index+1)
	r.updateCommitIndex()
	if r.leadTransferee != None && r.Prs[m.From].Match >= r.RaftLog.committed {
		// transferee的日志已经更新完毕，继续leader transfer的过程
		log.Infof("peer %v, transferee's Match = %v, r.committed = %v, r.lastLogIndex = %v, execute TransferLeader again",
			r.id, r.RaftLog.committed, r.RaftLog.LastIndex())
		r.Step(pb.Message{From: r.leadTransferee, MsgType: pb.MessageType_MsgTransferLeader})
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	if r.State != StateLeader || r.leadTransferee != None {
		// 本节点已经不是leader or 处于leader transfer过程中
		return
	}
	// log.Infof("term %v, peer %v, proposal a new entry[%v]", r.Term, r.id, m.Entries)
	r.RaftLog.AppendEntries(m.Entries, r.Term)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	for peer, _ := range r.Prs {
		if peer == r.id {
			r.updateCommitIndex()
			continue
		}
		r.sendAppend(peer)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	reply := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Reject:  true,
		From:    r.id,
		To:      m.From}
	if r.CheckTerm(m.Term, m.From) {
		reply.Reject = false
		r.electionElapsed = 0
	}
	reply.Term = r.Term
	r.msgs = append(r.msgs, reply)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if !m.Reject {
		// follower的日志落后了，马上发送AppendEntriesRPC推进follower的日志
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgPropose,
				Entries: []*pb.Entry{},
			})
		}
	}
}

func (r *Raft) handleMsgHup(m pb.Message) {
	if r.State == StateLeader {
		return
	}
	r.becomeCandidate()
	r.electionRejectNum = 0
	log.Infof("Term %v peer %v try to start a leader election", r.Term, r.id)
	for peer, _ := range r.Prs {
		if peer == r.id {
			// only 1 raft node in whole cluster
			if len(r.Prs) == 1 && r.State != StateLeader {
				r.becomeLeader()
				return
			}
			continue
		}
		r.sendVoteRequest(peer)
		// log.Debug("Term %v, peer %v send voteRequest to %v", r.Term, r.id, peer)
	}
}

func (r *Raft) handleMsgBeat(m pb.Message) {
	if r.State != StateLeader {
		return
	}
	for peer, _ := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Reject {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		}
		r.electionRejectNum++
		if r.electionRejectNum > uint64(len(r.Prs)/2) {
			r.becomeFollower(r.Term, None)
		}
		return
	}
	if r.State != StateCandidate || r.Term != m.Term {
		// 忽略过期的RPC应答
		return
	}
	r.votes[m.From] = true
	// log.Infof("Term %v, peer %v receive RequestResponse from peer %v response{%v}, votes{%v}", r.Term, r.id, m.From, m, r.votes)
	var voteNum int
	for _, voteGranted := range r.votes {
		if voteGranted {
			voteNum++
		}
		if voteNum > len(r.Prs)/2 && r.State != StateLeader {
			r.becomeLeader()
			break
		}
	}
}

func (r *Raft) CheckTerm(peerTerm uint64, lead uint64) bool {
	if r.Term > peerTerm {
		return false
	}
	if r.Term < peerTerm ||
		(lead != None && peerTerm == r.Term && r.State == StateCandidate) {
		r.becomeFollower(peerTerm, lead)
	}
	return true
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	reject := true
	MyLastLogIndex := r.RaftLog.LastIndex()
	MyLastLogTerm, _ := r.RaftLog.Term(MyLastLogIndex)
	if r.CheckTerm(m.Term, None) {
		if r.Vote == 0 || r.Vote == m.From {
			if m.LogTerm > MyLastLogTerm ||
				(m.LogTerm == MyLastLogTerm && m.Index >= MyLastLogIndex) {
				reject = false
				r.electionElapsed = 0
				r.Vote = m.From
			}
		}
	}
	log.Infof("Term %v, node %v REJECT{%v} to vote for candidate %v {m.Index = %v, m.LogTerm = %v, m.Term = %v},{myLastLogIndex = %v, myLastLogTerm = %v}, m{%v}",
		r.Term, r.id, reject, m.From, m.Index, m.LogTerm, m.Term, MyLastLogIndex, MyLastLogTerm, m)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Reject:  reject,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if r.CheckTerm(m.Term, m.From) {
		r.electionElapsed = 0
		r.RaftLog.pendingSnapshot = m.Snapshot
		r.RaftLog.updateTruncateMeta(m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term)
		// 更新集群信息
		r.votes = make(map[uint64]bool)
		r.Prs = make(map[uint64]*Progress)
		for _, peer := range m.Snapshot.Metadata.ConfState.Nodes {
			r.votes[peer] = false
			r.Prs[peer] = &Progress{}
		}
	}
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	// local Message，不需要检验Term
	// transferee存在m.From里面
	if _, ok := r.Prs[m.From]; !ok {
		// transferee不存在，就不用做任何事
		return
	}
	if r.State == StateLeader && r.Prs[m.From].Match < r.RaftLog.committed {
		// 不满足成为leader的条件（logEntry没有达到最新）
		r.leadTransferee = m.From
		r.sendAppend(m.From)
		log.Infof("peer %v transferLeader fail, transferee's log is stale, Match = %v, leader's LastIndex = %v",
			r.id, r.Prs[m.From].Match, r.RaftLog.committed)
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	})
	// log.Infof("peer %v send TimeoutNow Message to peer %v", r.id, m.From)
}

func (r *Raft) handleTimeoutNow(m pb.Message) {
	if r.id != m.To {
		panic("timeoutNow")
	}
	if _, ok := r.Prs[m.To]; !ok {
		// 当前节点已退出
		log.Infof("peer %v is exit, r.prs[%v]", m.To, r.Prs)
		return
	}
	if !r.CheckTerm(m.Term, None) {
		return
	}
	r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	if _, ok := r.Prs[id]; ok {
		// 重复的addNode请求，去重
		log.Infof("peer %v receive duplicate addNode request,ignore it", r.id)
		return
	}
	r.votes[id] = false
	// 初始化为1而不是0,主要是为了防止sendAppend中 prevLogIndex = Next - 1,导致prevLogIndex溢出
	r.Prs[id] = &Progress{Next: 1}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	if _, ok := r.Prs[id]; !ok {
		// 重复的removeNode请求，去重
		return
	}
	delete(r.Prs, id)
	delete(r.votes, id)
	if r.State == StateLeader {
		r.updateCommitIndex()
	}
	// 节点减少后，可能部分logEntry满足了大多数原则，可以抬升commitIndex
}

// 找到除了当前leader以外最新的那个节点，用于调用transferLeader
func (r *Raft) GetUpdateToDatePeer() uint64 {
	maxLogIndex := None
	newestPeer := None
	for Id, progress := range r.Prs {
		if r.id == Id {
			continue
		}
		if maxLogIndex < progress.Match {
			newestPeer = Id
			maxLogIndex = progress.Match
		}
	}
	return newestPeer
}
