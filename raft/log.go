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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	truncateIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	raftlog := &RaftLog{}
	raftlog.storage = storage
	raftlog.stabled, _ = storage.LastIndex()
	start, _ := storage.FirstIndex()
	end, _ := storage.LastIndex()
	raftlog.entries, _ = storage.Entries(start, end+1)
	hardState, _, _ := storage.InitialState()
	raftlog.committed = hardState.Commit
	raftlog.applied = start - 1
	raftlog.truncateIndex = start - 1
	return raftlog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() (ents []pb.Entry) {
	// may update after snapshot
	ents = make([]pb.Entry, 0)
	lastIndex := l.LastIndex()
	for i := l.stabled + 1; i <= lastIndex; i++ {
		ents = append(ents, l.entries[i-l.firstIndexInMem()])
	}
	return
}

func (l *RaftLog) firstIndexInMem() uint64 {
	if len(l.entries) > 0 {
		return l.entries[0].Index
	}
	panic("RaftLog::firstIndexInMem entries is empty")
}

// 删除的日志条目一定在commit之后，所以是放在raftLog.entries中（未持久化）
func (l *RaftLog) ClearEntsAfter(index uint64) {
	l.entries = l.entries[:(index - l.firstIndexInMem() + 1)]
}

func (l *RaftLog) AppendEntries(entries []*pb.Entry, term uint64) {
	if len(entries) == 0 {
		return
	}
	start := l.LastIndex() + 1
	nextIndex := start
	for _, entry := range entries {
		ent := *entry
		if ent.Index == 0 {
			ent.Index = nextIndex
		}
		if ent.Term == 0 {
			ent.Term = term
		}
		l.entries = append(l.entries, ent)
		nextIndex++
	}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// may update after installing snapshot module
	for i := l.applied + 1; i <= l.committed; i++ {
		ents = append(ents, l.entries[i-l.firstIndexInMem()])
	}
	return
}

// caller must ensure i > l.stabled
func (l *RaftLog) entsAfter(i uint64) (ents []*pb.Entry) {
	// may update after snapshot
	lastIndex := l.LastIndex()
	for ; i < lastIndex; i++ {
		ents = append(ents, &(l.entries[i-l.firstIndexInMem()+1]))
	}
	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	var lastIndex uint64
	if len(l.entries) > 0 {
		lastIndex = l.entries[len(l.entries)-1].Index
	} else {
		lastIndex = l.truncateIndex
	}
	return lastIndex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if len(l.entries) == 0 || i < l.firstIndexInMem() {
		return l.storage.Term(i)
	}
	return l.entries[i-l.firstIndexInMem()].Term, nil
}
