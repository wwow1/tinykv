package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	resp := new(kvrpcpb.RawGetResponse)
	reader, _ := server.storage.Reader(req.GetContext())
	defer reader.Close()
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if val == nil {
		// key not found
		resp.NotFound = true
	}
	if err != nil {
		// other error
		resp.Error = err.Error()
	}
	resp.Value = val
	return resp, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	resp := new(kvrpcpb.RawPutResponse)
	modify := storage.Modify{
		Data: storage.Put{
			Key:   req.GetKey(),
			Value: req.GetValue(),
			Cf:    req.GetCf(),
		}}
	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := new(kvrpcpb.RawDeleteResponse)
	modify := storage.Modify{
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf:  req.GetCf(),
		}}
	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	var err error
	resp := new(kvrpcpb.RawScanResponse)
	reader, _ := server.storage.Reader(req.GetContext())
	defer reader.Close()
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	for iter.Seek(req.GetStartKey()); iter.Valid() && len(resp.Kvs) < int(req.GetLimit()); iter.Next() {
		var kvpair kvrpcpb.KvPair
		kvpair.Key = iter.Item().Key()
		kvpair.Value, err = iter.Item().Value()
		if err != nil {
			break
		} else {
			resp.Kvs = append(resp.Kvs, &kvpair)
		}
	}
	return resp, err
}
