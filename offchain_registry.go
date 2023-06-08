package offchain_transmission

import (
	"context"
	"fmt"
	"github.com/guozhuang333/bitxhub-core/agency"
	"github.com/meshplus/bitxhub-model/pb"
	"github.com/sirupsen/logrus"
)

type OffChainTransmissionMgr struct {
	peerMgr      agency.PeerManager
	logger       *logrus.Entry
	appchainID   string
	savePath     string
	remotePierID []string
	reqCh        chan *pb.GetDataRequest
	client       agency.Client
	ctx          context.Context
	cancel       context.CancelFunc
}

type Payload struct {
	Ok   bool
	Data []byte
}

const FILECHUNK = 1 << 29

func New() agency.OffChainTransmission {
	return &OffChainTransmissionMgr{}
}

func init() {
	agency.RegisterOffchainTransmissionConstructor("offChain_transmission", New)
}

func (o *OffChainTransmissionMgr) Start() error {
	//if err := o.peerMgr.RegisterMsgHandler(pb.Message_OFFCHAIN_DATA_GET, o.handleGetOffChainData); err != nil {
	//	return fmt.Errorf("register get offchain data request msg handler: %w", err)
	//}
	//
	//if err := o.peerMgr.RegisterMsgHandler(pb.Message_OFFCHAIN_DATA_SEND, o.handleReceivedOffChainData); err != nil {
	//	return fmt.Errorf("register handle offchain data msg handler: %w", err)
	//}
	//
	//if err := o.peerMgr.RegisterMsgHandler(pb.Message_ADDRESS_GET, o.handleGetAddressMessage); err != nil {
	//	return fmt.Errorf("register get address msg handler: %w", err)
	//}

	if err := o.peerMgr.Start(); err != nil {
		return fmt.Errorf("peerMgr start: %w", err)
	}

	o.logger.Info("测试成功")

	return nil
}

func (o *OffChainTransmissionMgr) Stop() error {
	o.cancel()

	return nil
}

func (o *OffChainTransmissionMgr) test(a int, b int) int {
	return a + b
}

//func (o *OffChainTransmissionMgr) startListenOffChainDataReq() {
//	o.logger.Info("start listen offChain data get request")
//	for {
//		select {
//		case req := <-o.reqCh:
//			o.logger.Info("receive offChain data get req")
//			// check remote chain ID
//			_, targetChainID, _, err := pb.ParseFullServiceID(req.To)
//			if err != nil {
//				o.logger.Warnf("parse full service ID: %s", err.Error())
//				return
//			}
//
//			flag := false
//			for _, conn := range o.remotePierID {
//				if conn == targetChainID {
//					flag = true
//				}
//			}
//			if !flag {
//				o.logger.Warnf("target pier %s is disconnected", targetChainID)
//				return
//			}
//
//			// send offchain data request
//			o.logger.Info("start send offChain data get req")
//			if err := retry.Retry(func(attempt uint) error {
//				if attempt != 0 && attempt%3 == 0 {
//					o.logger.Errorf("get offChain data failed")
//					return nil
//				}
//
//				data, err := req.Marshal()
//				if err != nil {
//					o.logger.Panicf("marshal offChain data get req: %s", err.Error())
//				}
//				entry := o.logger.WithFields(logrus.Fields{
//					"from":  req.From,
//					"to":    req.To,
//					"index": req.Index,
//				})
//				msg := &pb.Message{
//					Type: pb.Message_OFFCHAIN_DATA_GET,
//					Data: data,
//				}
//				res, err := o.peerMgr.Send(targetChainID, msg)
//				if err != nil {
//					entry.Error(err)
//					return err
//				}
//				entry.Info("send offChain data get request success")
//				pd := &Payload{}
//				if err := json.Unmarshal(res.Data, pd); err != nil {
//					return err
//				}
//				if !pd.Ok {
//					// encounter an error, retry
//					entry.Error("dst chain failed to process request: %s", string(res.Data))
//					return err
//				}
//
//				return nil
//			}, strategy.Wait(10*time.Second)); err != nil {
//				o.logger.Panic(err)
//			}
//		case <-o.ctx.Done():
//			o.logger.Infof("receiving done signal, exit offchain transmission...")
//			return
//		}
//	}
//}
//
//// get offChain data and send back
//func (o *OffChainTransmissionMgr) handleGetOffChainData(stream network.Stream, msg *pb.Message) {
//	var req = &pb.GetDataRequest{}
//	if err := req.Unmarshal(msg.Data); err != nil {
//		o.logger.Error(err)
//		o.sendErrMessage(stream, err.Error())
//		return
//	}
//	entry := o.logger.WithFields(logrus.Fields{
//		"from":  req.From,
//		"to":    req.To,
//		"index": req.Index,
//	})
//	entry.Info("receive offChain data get req, start get offChain data")
//
//	// get offChain data info
//	// download offChain data & send back
//	res, err := o.client.GetOffChainData(req)
//	if err != nil {
//		entry.Error(err)
//		o.sendErrMessage(stream, err.Error())
//		return
//	}
//
//	f, err := os.Open(res.Filepath)
//	defer f.Close()
//	if err != nil {
//		entry.Error(err)
//		o.sendErrMessage(stream, err.Error())
//	}
//
//	retMsg := constructMessage(pb.Message_ACK, true, []byte("success"))
//	err = o.peerMgr.AsyncSendWithStream(stream, retMsg)
//	if err != nil {
//		o.logger.Error(err)
//	}
//
//	// transmission with shards
//	target := strings.Split(req.From, ":")[1]
//	shardSize := uint64(math.Ceil(float64(res.Filesize) / float64(FILECHUNK)))
//	resp := &pb.GetDataResponse{
//		Index: req.Index,
//		From:  req.From,
//		To:    req.To,
//		Type:  pb.GetDataResponse_DATA_GET_SUCCESS,
//		Msg:   res.Filename,
//	}
//	for i := uint64(0); i < shardSize; i++ {
//		partSize := int(math.Min(FILECHUNK, float64(res.Filesize-int64(i*FILECHUNK))))
//		shard := make([]byte, partSize)
//		f.Read(shard)
//		if err := retry.Retry(func(attempt uint) error {
//			resp.Data = shard
//			resp.ShardTag = &pb.ShardIdentification{
//				IsShard:    true,
//				ShardIndex: i + 1,
//				ShardSize:  shardSize,
//			}
//			respByte, err := resp.Marshal()
//			if err != nil {
//				entry.Error(err)
//				return nil
//			}
//			msg := &pb.Message{
//				Type: pb.Message_OFFCHAIN_DATA_SEND,
//				Data: respByte,
//			}
//			res, err := o.peerMgr.Send(target, msg)
//			pd := &Payload{}
//			if err := json.Unmarshal(res.Data, pd); err != nil {
//				entry.Error(err)
//				return nil
//			}
//			if !pd.Ok {
//				entry.Errorf("send offChain data %d/%d failed: %s", i+1, shardSize, string(pd.Data))
//				return err
//			}
//			return nil
//		}, strategy.Wait(10*time.Second)); err != nil {
//			o.logger.Panic(err)
//		}
//	}
//}
//
//// receive offChain data and save
//func (o *OffChainTransmissionMgr) handleReceivedOffChainData(stream network.Stream, msg *pb.Message) {
//	resp := &pb.GetDataResponse{}
//	if err := resp.Unmarshal(msg.Data); err != nil {
//		o.logger.Error(err)
//		o.sendErrMessage(stream, err.Error())
//		return
//	}
//
//	entry := o.logger.WithFields(logrus.Fields{
//		"from":  resp.From,
//		"to":    resp.To,
//		"index": resp.Index,
//	})
//	entry.Infof("receive offChain data %d/%d", resp.ShardTag.ShardIndex, resp.ShardTag.ShardSize)
//
//	// save offChain data
//	// path depends on the circumstances
//	// modify here as required
//	name := fmt.Sprintf("%s-%s-%d-%d-%d", resp.From, resp.To, resp.Index, resp.ShardTag.ShardIndex, resp.ShardTag.ShardSize)
//	path := filepath.Join(o.savePath, name)
//	if err := ioutil.WriteFile(path, resp.Data, 0644); err != nil {
//		entry.Errorf("save offChain data: %s", err.Error())
//		o.sendErrMessage(stream, err.Error())
//		return
//	}
//
//	// received all the offChain data, notify plugin to process
//	if resp.ShardTag.ShardIndex == resp.ShardTag.ShardSize {
//		resp.Data = []byte(o.savePath)
//		if err := o.client.SubmitOffChainData(resp); err != nil {
//			entry.Error(err)
//			return
//		}
//	}
//
//	retMsg := constructMessage(pb.Message_ACK, true, []byte("success"))
//	err := o.peerMgr.AsyncSendWithStream(stream, retMsg)
//	if err != nil {
//		o.logger.Error(err)
//	}
//}
//
//func (o *OffChainTransmissionMgr) sendErrMessage(stream network.Stream, msg string) {
//	retMsg := constructMessage(pb.Message_ACK, false, []byte(msg))
//	err := o.peerMgr.AsyncSendWithStream(stream, retMsg)
//	if err != nil {
//		o.logger.Error(err)
//	}
//}
//
//func (o *OffChainTransmissionMgr) handleGetAddressMessage(stream network.Stream, message *pb.Message) {
//	addr := o.appchainID
//
//	retMsg := constructMessage(pb.Message_ACK, true, []byte(addr))
//
//	err := o.peerMgr.AsyncSendWithStream(stream, retMsg)
//	if err != nil {
//		o.logger.Error(err)
//		return
//	}
//
//	pd := &Payload{}
//	if err := json.Unmarshal(message.Data, pd); err != nil {
//		o.logger.Infof("unmarshal message payload failed: %s", err.Error())
//	}
//
//	connectInfo := &pb.ConnectInfo{}
//	addrInfo := &peer.AddrInfo{}
//	if err := connectInfo.Unmarshal(pd.Data); err != nil {
//		o.logger.Errorf("unmarshal connectInfo failed: %s", err.Error())
//	}
//	if err := addrInfo.UnmarshalJSON(connectInfo.AddrInfo); err != nil {
//		o.logger.Errorf("unmarshal addrInfo failed: %s", err.Error())
//	}
//
//	needConnect := true
//	for _, pierId := range o.remotePierID {
//		if strings.EqualFold(pierId, connectInfo.PierId) {
//			needConnect = false
//		}
//	}
//
//	if needConnect {
//		_, err := o.peerMgr.Connect(addrInfo)
//		if err != nil {
//			o.logger.Errorf("connect to %s failed: %s", connectInfo.PierId, err.Error())
//		}
//		o.remotePierID = append(o.remotePierID, connectInfo.PierId)
//	}
//}
//
//func constructMessage(typ pb.Message_Type, ok bool, data []byte) *pb.Message {
//	payload := &Payload{Ok: ok, Data: data}
//	mData, err := json.Marshal(payload)
//	if err != nil {
//		return nil
//	}
//	return &pb.Message{
//		Type: typ,
//		Data: mData,
//	}
//}
