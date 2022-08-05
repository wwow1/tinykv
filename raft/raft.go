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
	for _, peer := range c.peers {
		raft.votes[peer] = false
		raft.Prs[peer] = &Progress{}
	}
	raft.Term = hardState.Term
	raft.Vote = hardState.Vote

	storageLastIndex, err := raft.RaftLog.storage.LastIndex()
	if err != nil {
		panic("xxx")
	}
	log.Infof("peer %v start, Term %v, committed = %v, lastIndex = %v, stabled = %v, applied = %v, storageLastIndex = %v", raft.id, raft.Term,
		raft.RaftLog.committed, raft.RaftLog.LastIndex(), raft.RaftLog.stabled, raft.RaftLog.applied, storageLastIndex)
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	if prevLogIndex > r.RaftLog.LastIndex() {
		panic("Raft::sendAppend prevLogIndex > lastIndex")
	}
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
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
		Commit:  r.RaftLog.committed,
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
			r.electionElapsed = 0
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
	// log.Infof("Term %v, peer %v become candidate, and start a leaderElection", r.Term, r.id)
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
	log.Infof("node %d become leader, term %d", r.id, r.Term)
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
	if m.To != 0 && m.To != r.id {
		// DEBUG
		m.To = r.id
		log.Infof("m.To(%v) != r.id(%v), m{%v}", m.To, r.id, m)
		panic("m.To != r.id")
	}
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State == StateLeader {
			return nil
		}
		r.becomeCandidate()
		r.electionRejectNum = 0
		for peer, _ := range r.Prs {
			if peer == r.id {
				// only 1 raft node in whole cluster
				if len(r.Prs) == 1 && r.State != StateLeader {
					r.becomeLeader()
					return nil
				}
				continue
			}
			r.sendVoteRequest(peer)
			// log.Infof("Term %v, peer %v send voteRequest to %v", r.Term, r.id, peer)
		}
	case pb.MessageType_MsgBeat:
		if r.State != StateLeader {
			return nil
		}
		for peer, _ := range r.Prs {
			if peer == r.id {
				continue
			}
			r.sendHeartbeat(peer)
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Reject {
			if m.Term > r.Term {
				r.becomeFollower(m.Term, None)
			}
			r.electionRejectNum++
			if r.electionRejectNum > uint64(len(r.Prs)/2) {
				r.becomeFollower(r.Term, None)
			}
			return nil
		}
		if r.State != StateCandidate || r.Term != m.Term {
			// 忽略过期的RPC应答
			return nil
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
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		if r.State != StateLeader {
			return nil
		}
		if m.Reject {
			// m.Index = 0也意味着对方的Term比本次rpc发送时携带的Term大
			//(但是rpc返回的时候可能本节点的Term也变大了，导致错误判断并进入quickGoBackIndex)
			if m.Term > r.Term || m.Index == 0 {
				r.becomeFollower(m.Term, None)
				log.DPrintf("leader %v failed to send appendEntryiesRPC to %v, its term %v is higher than leader's term %v",
					r.id, m.From, m.Term, r.Term)
			} else {
				r.quickGoBackIndex(m.Index, m.LogTerm, m.From)
				r.sendAppend(m.From)
				log.DPrintf("leader %v failed to send appendEntryiesRPC to %v, quickGoBackIndex to %v",
					r.id, m.From, r.Prs[m.From].Next)
			}
			return nil
		}
		if r.Term != m.Term {
			// 忽略过期的RPC应答
			return nil
		}
		if m.Index > r.RaftLog.LastIndex()+1 {
			panic("Raft::Step reply.Index > myLastIndex")
		}
		r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
		r.Prs[m.From].Next = max(r.Prs[m.From].Next, m.Index+1)
		r.updateCommitIndex()
	case pb.MessageType_MsgPropose:
		if r.State != StateLeader {
			return &util.ErrNotLeader{}
		}
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
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
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
	return nil
}

func (r *Raft) updateCommitIndex() {
	var matchArr uint64Slice
	for _, pr := range r.Prs {
		matchArr = append(matchArr, pr.Match)
	}
	sort.Sort(matchArr)
	olderCommitted := r.RaftLog.committed
	for wantCommitted := matchArr[(len(matchArr)-1)/2]; wantCommitted > 0; wantCommitted-- {
		// 只允许提交当前周期的entry
		logTerm, _ := r.RaftLog.Term(wantCommitted)
		if logTerm == r.Term {
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
	prevLogTerm, _ := r.RaftLog.Term(m.Index)
	if prevLogTerm != m.LogTerm {
		//PrevLogIndex处的日志项的周期号不匹配
		reply.LogTerm = prevLogTerm // conflictTerm
		// 产生冲突,回退stabled TODO(zhengfuyu): maybe error
		r.RaftLog.stabled = min(r.RaftLog.stabled, m.Index-1)
		if m.Index == 0 {
			reply.Index = 1
		} else {
			for i := m.Index - 1; i >= 0; i-- {
				myLogTerm, _ := r.RaftLog.Term(i)
				if myLogTerm < reply.LogTerm {
					reply.Index = i + 1 // conflictIndex
					break
				}
			}
		}
		return
	}
	leaderLastLogIndex := m.Index + uint64(len(m.Entries))
	for i := m.Index + 1; i <= r.RaftLog.LastIndex() && i <= leaderLastLogIndex; i++ {
		//找到不匹配的日志项，删除在它之后的所有日志项
		myLogTerm, _ := r.RaftLog.Term(i)
		if myLogTerm != m.Entries[i-m.Index-1].Term {
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
	if m.Commit > r.RaftLog.committed {
		lastNewEntry := m.Index
		if len(m.Entries) != 0 {
			lastNewEntry = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.committed = min(m.Commit, lastNewEntry)
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
	// log.Infof("Term %v, node %v REJECT{%v} to vote for candidate %v {m.Index = %v, m.LogTerm = %v, m.Term = %v},{myLastLogIndex = %v, myLastLogTerm = %v}, m{%v}",
	// 	r.Term, r.id, reject, m.From, m.Index, m.LogTerm, m.Term, MyLastLogIndex, MyLastLogTerm, m)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Reject:  reject,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	})
}

func (r *Raft) quickGoBackIndex(conflictIndex, conflictTerm, peer uint64) {
	/*
		if conflictTerm != None {
			// TODO(zhengfuyu): 二分法优化？
			for i := r.RaftLog.LastIndex(); i > 0; i-- {
				logTerm, _ := r.RaftLog.Term(i)
				if logTerm < conflictTerm {
					r.Prs[peer].Next = i + 1
					return
				}
			}
		}
	*/
	r.Prs[peer].Next = conflictIndex
	// DEBUG
	if r.Prs[peer].Next == 0 {
		panic("Raft::quickGoBackIndex next = 0")
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
