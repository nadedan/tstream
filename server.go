package tstream

import (
	"context"
	"fmt"
	"math/rand"

	"tstream/rpc"

	"google.golang.org/grpc"
)

type DataSrc interface {
	Get([]string) ([]uint32, error)
}

const (
	globalTriggerId = 0xDEADBEEF
)

type TriggerId uint32
type StreamId uint32

var pull struct{} = struct{}{}

type Trigger chan struct{}

type Stream struct {
	trigger Trigger
	signals []string
}

type Server struct {
	data     DataSrc
	triggers map[TriggerId]Trigger
	streams  map[StreamId]Stream
}

func NewServer(data DataSrc) *Server {
	s := &Server{}

	s.data = data

	s.streams = make(map[StreamId]Stream)

	s.triggers = make(map[TriggerId]Trigger)
	s.triggers[globalTriggerId] = make(Trigger)

	return s
}

func (s *Server) NewTrigger(context.Context, *rpc.MsgVoid) (*rpc.MsgTrigger, error) {
	resp := &rpc.MsgTrigger{}

	var id TriggerId
	exists := true
	for id == 0 || exists {
		id = TriggerId(rand.Uint32())
		_, exists = s.triggers[id]
	}

	s.triggers[id] = make(Trigger)

	resp.Id = uint32(id)

	return resp, nil
}
func (s *Server) Trigger(in grpc.ClientStreamingServer[rpc.MsgTrigger, rpc.MsgVoid]) error {

	for {
		msg, err := in.Recv()
		if err != nil {
			return fmt.Errorf("%T.Trigger: %w", s, err)
		}

		trigger, ok := s.triggers[TriggerId(msg.GetId())]
		if !ok {
			return fmt.Errorf("%T.Trigger: bad trigger id %d", s, msg.GetId())
		}

		select {
		case trigger <- pull:
		default:
		}
	}
}

func (s *Server) NewStream(ctx context.Context, req *rpc.MsgStreamReq) (*rpc.MsgStream, error) {
	resp := &rpc.MsgStream{}

	trigger, ok := s.triggers[TriggerId(req.GetTriggerId())]
	if !ok {
		return resp, fmt.Errorf("%T.Trigger: bad trigger id %d", s, req.GetTriggerId())
	}

	var id StreamId
	exists := true
	for id == 0 || exists {
		id = StreamId(rand.Uint32())
		_, exists = s.streams[id]
	}

	s.streams[id] = Stream{trigger: trigger, signals: req.GetSignals()}

	return resp, nil
}

func (s *Server) Stream(req *rpc.MsgStream, out grpc.ServerStreamingServer[rpc.MsgData]) error {
	stream, ok := s.streams[StreamId(req.GetId())]
	if !ok {
		return fmt.Errorf("%T.Trigger: bad stream id %d", s, req.GetId())
	}

	for {
		_, ok := <-stream.trigger
		if !ok {
			return nil
		}

		data, err := s.data.Get(stream.signals)
		if err != nil {
			return fmt.Errorf("%T.Stream: could not get data: %w", s, err)
		}
		msg := &rpc.MsgData{}
		msg.Values = data

		err = out.Send(msg)
		if err != nil {
			return fmt.Errorf("%T.Stream: could not send: %w", s, err)
		}
	}
}
