package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) findProposal(ent *eraftpb.Entry) (cb *message.Callback) {
	for idx, proposal := range d.proposals {
		if proposal.index == ent.Index {
			if proposal.term != ent.Term {
				// 该proposal对应的命令没有被raft提交（超时或是其他原因）
				log.Infof("peer %v delete stale proposal %v", d.Meta.Id, proposal)
				NotifyStaleReq(ent.Term, proposal.cb)
			} else {
				cb = proposal.cb
				d.proposals = append(d.proposals[:idx], d.proposals[idx+1:]...)
				return
			}
		}
	}
	return nil
}

func (d *peerMsgHandler) CheckKeyInRegion(resp *raft_cmdpb.RaftCmdResponse, key *[]byte) bool {
	err := util.CheckKeyInRegion(*key, d.Region())
	if err == nil {
		return true
	}
	log.Infof("key not in region")
	if resp.Header.Error == nil {
		resp.Header.Error = &errorpb.Error{}
	}
	resp.Header.Error.KeyNotInRegion = &errorpb.KeyNotInRegion{
		Key:      *key,
		RegionId: d.regionId,
		StartKey: d.Region().StartKey,
		EndKey:   d.Region().EndKey,
	}
	return false
}

func (d *peerMsgHandler) extractKey(req *raft_cmdpb.Request) []byte {
	var key []byte
	switch req.CmdType {
	case raft_cmdpb.CmdType_Delete:
		key = req.Delete.Key
	case raft_cmdpb.CmdType_Put:
		key = req.Put.Key
	case raft_cmdpb.CmdType_Get:
		key = req.Get.Key
	default:
	}
	return key
}

func (d *peerMsgHandler) applyRWRequest(kvWB *engine_util.WriteBatch, resp *raft_cmdpb.RaftCmdResponse, reqs *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	for _, req := range reqs.Requests {
		key := d.extractKey(req)
		if key != nil && !d.CheckKeyInRegion(resp, &key) {
			return
		}
		switch req.CmdType {
		case raft_cmdpb.CmdType_Delete:
			kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
			resp.Responses = []*raft_cmdpb.Response{
				{CmdType: raft_cmdpb.CmdType_Delete,
					Delete: &raft_cmdpb.DeleteResponse{}},
			}
		case raft_cmdpb.CmdType_Put:
			kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
			resp.Responses = []*raft_cmdpb.Response{
				{CmdType: raft_cmdpb.CmdType_Put,
					Put: &raft_cmdpb.PutResponse{}},
			}
		case raft_cmdpb.CmdType_Get:
			val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
			if err != nil {
				panic("peerMsgHandler::processEntry GetCF")
			}
			resp.Responses = []*raft_cmdpb.Response{
				{CmdType: raft_cmdpb.CmdType_Get,
					Get: &raft_cmdpb.GetResponse{Value: val}},
			}
		case raft_cmdpb.CmdType_Snap:
			if reqs.Header.RegionEpoch.ConfVer != d.Region().RegionEpoch.ConfVer ||
				reqs.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
				if resp.Header.Error == nil {
					resp.Header.Error = &errorpb.Error{}
				}
				resp.Header.Error.EpochNotMatch = &errorpb.EpochNotMatch{CurrentRegions: []*metapb.Region{d.Region()}}
				return
			}
			resp.Responses = []*raft_cmdpb.Response{
				{CmdType: raft_cmdpb.CmdType_Snap,
					Snap: &raft_cmdpb.SnapResponse{Region: d.Region()}},
			}
			if cb != nil {
				cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
			}
		case raft_cmdpb.CmdType_Invalid:
			panic("peerMsgHandler::processEntry CmdType_Invalid")
		default:
		}
	}
}

func (d *peerMsgHandler) applyAdminRequest(kvWB *engine_util.WriteBatch, resp *raft_cmdpb.RaftCmdResponse, req *raft_cmdpb.AdminRequest) {
	if req != nil {
		switch req.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			if d.peerStorage.applyState.TruncatedState.Index > req.CompactLog.CompactIndex {
				log.Debug("peer %v receive a staled compactLog CMD, applyState{%v}, CompactLog{%v}", d.peer.Meta.Id, d.peerStorage.applyState, req.CompactLog)
				return
			}
			// log.Debug("peer %v apply snapshot{%v}", d.peer.Meta.Id, req.CompactLog)
			d.peerStorage.applyState.TruncatedState.Index = req.CompactLog.CompactIndex
			d.peerStorage.applyState.TruncatedState.Term = req.CompactLog.CompactTerm
			err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if err != nil {
				panic("peerMsgHandler::applyAdminRequest SetMeta")
			}
			d.ScheduleCompactLog(d.peerStorage.applyState.TruncatedState.Index)
		case raft_cmdpb.AdminCmdType_Split:
			oldRegion := d.Region()
			// 前置检查
			err := util.CheckKeyInRegion(req.Split.SplitKey, oldRegion)
			if err != nil {
				log.Infof("peer %v ignore a duplicate split command {%v}", d.Meta.Id, req)
				return
			}
			var kvWB engine_util.WriteBatch
			defer kvWB.MustWriteToDB(d.ctx.engine.Kv)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			defer storeMeta.Unlock()
			storeMeta.regionRanges.Delete(&regionItem{region: oldRegion})
			oldRegion.RegionEpoch.Version++

			// 创建新的Region
			newRegion := new(metapb.Region)
			err = util.CloneMsg(d.Region(), newRegion)
			if err != nil {
				return
			}
			oldRegion.EndKey = req.Split.SplitKey
			newRegion.Id = req.Split.NewRegionId
			newRegion.StartKey = req.Split.SplitKey
			// newRegion的peers需要怎么设置(TODO)
			newRegion.Peers = make([]*metapb.Peer, 0)
			oldPeers := oldRegion.GetPeers()
			for idx, newPeerId := range req.Split.NewPeerIds {
				newRegion.Peers = append(newRegion.Peers, &metapb.Peer{Id: newPeerId, StoreId: oldPeers[idx].StoreId})
			}
			// 将新旧两个region结构持久化
			meta.WriteRegionState(&kvWB, oldRegion, rspb.PeerState_Normal)
			meta.WriteRegionState(&kvWB, newRegion, rspb.PeerState_Normal)
			newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
			if err != nil {
				panic(err)
			}
			// 向router注册新的peer结构(这样发送过来的RaftMessage才能被peer接收到)
			d.ctx.router.register(newPeer)
			// 启动新分裂出来的peer
			_ = d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})
			// 将region信息更新到storeMeta中
			storeMeta.setRegion(newRegion, newPeer)
			storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: oldRegion})
			storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
			// log.Infof("peer %v split region, oldRegion[%v], newRegion[%v]", d.Meta.Id, oldRegion, newRegion)
			if d.IsLeader() {
				d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
			}
		default:
		}
	}
}

func (d *peerMsgHandler) processTransferLeader(leader uint64, cb *message.Callback) {
	d.RaftGroup.TransferLeader(leader)
	resp := raft_cmdpb.RaftCmdResponse{Header: newCmdResp().Header}
	resp.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType(raft_cmdpb.AdminCmdType_TransferLeader),
		TransferLeader: &raft_cmdpb.TransferLeaderResponse{}}
	cb.Done(&resp)
}

func (d *peerMsgHandler) addNode(changePeer *metapb.Peer, confChange *eraftpb.ConfChange) {
	for _, peer := range d.peerStorage.region.Peers {
		if peer.Id == changePeer.Id {
			// 重复的AddNode请求，无视
			return
		}
	}
	d.peerStorage.region.Peers = append(d.peerStorage.region.Peers, changePeer)
	// 发送RaftMessage的时候需要从peerCache中获取节点信息，如果不更新这个结构到时候就无法发送RaftMessage
	d.insertPeerCache(changePeer)
	d.peerStorage.region.RegionEpoch.ConfVer++
	// 向PeersStartPendingTime中添加新节点的信息
	d.PeersStartPendingTime[changePeer.Id] = time.Now()
	d.RaftGroup.ApplyConfChange(*confChange)
	log.Infof("peer[%v] add other-peer[%v] to region[%v], now region-peers[%v], regionConfVer[%d]",
		d.Meta.Id, confChange.NodeId, d.regionId, d.Region().Peers, d.peerStorage.region.RegionEpoch.ConfVer)

	if !d.stopped && d.IsLeader() {
		// 每次触发ConfChange后，立刻向scheduler发送心跳更新region
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		log.Infof("peer[%v] send heartBeat to scheduler, region[%v]", d.Meta.Id, d.peerStorage.region)
	}
}

func (d *peerMsgHandler) removeNode(changePeer *metapb.Peer, confChange *eraftpb.ConfChange) {
	for i, peer := range d.peerStorage.region.Peers {
		if peer.Id != changePeer.Id {
			continue
		}
		/*
			if d.peer.RaftGroup.Raft.Lead == changePeer.Id {
				if d.Meta.Id != d.peer.RaftGroup.Raft.Lead {
					return
				}
				peerId := d.RaftGroup.Raft.GetUpdateToDatePeer()
				d.processTransferLeader(peerId, nil)
				log.Infof("peer %v want to delete peer %v, but it is leader,we should first transfer leader to peer %v",
					d.Meta.Id, changePeer.Id, peerId)
				return
			}
		*/
		if len(d.peerStorage.region.Peers) == 2 && d.peer.RaftGroup.Raft.Lead == changePeer.Id {
			// 针对场景：只剩2节点，且要删除的节点是leader
			// 需要暂时拒绝本次removeNode请求,并且向另一个follower节点发送transferLeader请求转移leader
			if d.Meta.Id != d.peer.RaftGroup.Raft.Lead {
				return
			}
			d.processTransferLeader(d.peerStorage.region.Peers[(i+1)%2].Id, nil)
			log.Infof("peer %v want to delete peer %v, but it is leader,we should first transfer leader to peer %v",
				d.Meta.Id, changePeer.Id, d.peerStorage.region.Peers[(i+1)%2].Id)
			return
		}
		d.removePeerCache(d.Meta.Id)
		if changePeer.Id == d.Meta.Id {
			// 如果被删除的节点是自己，那么删除本节点的peer结构
			d.destroyPeer()
			d.peerStorage.region.Peers = append(d.peerStorage.region.Peers[:i], d.peerStorage.region.Peers[i+1:]...)
			d.peerStorage.region.RegionEpoch.ConfVer++
			return
		}
		d.peerStorage.region.Peers = append(d.peerStorage.region.Peers[:i], d.peerStorage.region.Peers[i+1:]...)
		d.peerStorage.region.RegionEpoch.ConfVer++
		d.RaftGroup.ApplyConfChange(*confChange)
		log.Infof("peer[%v] remove other-peer[%v] to region[%v], now region-peers[%v], regionConfVer[%d]",
			d.Meta.Id, confChange.NodeId, d.regionId, d.Region().Peers, d.peerStorage.region.RegionEpoch.ConfVer)

		if !d.stopped && d.IsLeader() {
			// 每次触发ConfChange后，立刻向scheduler发送心跳更新region
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
			log.Infof("peer[%v] send heartBeat to scheduler, region[%v]", d.Meta.Id, d.peerStorage.region)
		}
		return
	}
}

func (d *peerMsgHandler) processConfigChange(ent eraftpb.Entry) {
	var confChange eraftpb.ConfChange
	err := proto.Unmarshal(ent.Data, &confChange)
	if err != nil {
		panic("peerMsgHandler::processConfigChange unmarshal confChange")
	}
	var changePeer metapb.Peer
	err = proto.Unmarshal(confChange.Context, &changePeer)
	if err != nil {
		panic("peerMsgHandler::processConfigChange unmarshal raftCmdRequest")
	}
	if changePeer.Id != confChange.NodeId {
		panic("peerMsgHandler::processConfigChange changePeer.Id != confChange.NodeId")
	}

	var kvWB engine_util.WriteBatch
	defer kvWB.MustWriteToDB(d.ctx.engine.Kv)

	if confChange.ChangeType == eraftpb.ConfChangeType_AddNode {
		d.addNode(&changePeer, &confChange)
	} else {
		d.removeNode(&changePeer, &confChange)
	}

	if !d.stopped {
		// 如果peer已经被销毁，不能再往磁盘写入regionState(会覆盖正确的状态信息)
		meta.WriteRegionState(&kvWB, d.peerStorage.region, rspb.PeerState_Normal)
	}
}

func (d *peerMsgHandler) processEntry(ent eraftpb.Entry) {
	var req raft_cmdpb.RaftCmdRequest
	proto.Unmarshal(ent.Data, &req)
	cb := d.findProposal(&ent)
	var kvWB engine_util.WriteBatch
	defer kvWB.MustWriteToDB(d.ctx.engine.Kv)
	resp := raft_cmdpb.RaftCmdResponse{Header: newCmdResp().Header}
	if ent.EntryType == eraftpb.EntryType_EntryNormal {
		d.applyRWRequest(&kvWB, &resp, &req, cb)
		d.applyAdminRequest(&kvWB, &resp, req.AdminRequest)
	} else {
		// EntryType_EntryConfChange
		d.processConfigChange(ent)
	}
	if !d.stopped && ent.Index > d.peerStorage.applyState.AppliedIndex {
		d.peerStorage.applyState.AppliedIndex = ent.Index
		err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		if err != nil {
			panic("peerMsgHandler::processEntry SetMeta")
		}
	}
	cb.Done(&resp)
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	if d.RaftGroup.HasReady() {
		rd := d.RaftGroup.Ready()
		snapshotApplyResult, err := d.peerStorage.SaveReadyState(&rd)
		if err != nil {
			panic("peerMsgHandler::HandleRaftReady SaveReadyState")
		}
		if snapshotApplyResult != nil {
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regionRanges.Delete(&regionItem{region: snapshotApplyResult.PrevRegion})
			storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: snapshotApplyResult.Region})
			// 不能defer Unlock,因为在processEntry中的Split和ConfigChange都可能调用Lock()导致死锁
			storeMeta.Unlock()
		}
		d.Send(d.ctx.trans, rd.Messages)
		for _, ent := range rd.CommittedEntries {
			d.processEntry(ent)
			// 执行完removeNode请求后，可能当前peer被销毁，停止后续所有操作
			if d.stopped {
				return
			}
		}
		d.RaftGroup.Advance(rd)
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	val, err := proto.Marshal(msg)
	if err != nil {
		panic("peerMsgHandler::proposeRaftCommand Marshal")
	}
	if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_TransferLeader {
		// TransferLeader命令不需要复制到其他节点上执行，本节点直接调用接口即可
		d.processTransferLeader(msg.AdminRequest.TransferLeader.Peer.Id, cb)
		return
	}
	d.proposals = append(d.proposals, &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	})
	if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_ChangePeer {
		rawPeer, err := proto.Marshal(msg.AdminRequest.ChangePeer.Peer)
		if err != nil {
			panic("ChangePeer.Peer Marshal fail")
			cb.Done(ErrResp(err))
			return
		}
		err = d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
			ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
			NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
			Context:    rawPeer})
		if err != nil {
			cb.Done(ErrResp(err))
		}
		return
	}
	err = d.RaftGroup.Propose(val)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale epoch[%v], current %v ignore it",
			regionID, msgType, msg.GetRegionEpoch(), curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
