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
	globalTriggerId = 0x00610BA1
)

type TriggerId uint32
type StreamId uint32

var pull struct{} = struct{}{}

type Trigger struct {
	channel     chan struct{}
	fanouts     map[int32]*fanout
	chNewFanout chan *fanout
	chRemFanout chan *fanout
}

type Stream struct {
	trigger Trigger
	signals []string
}

type Server struct {
	rpc.UnimplementedTriggeredStreamServer

	data DataSrc

	triggers map[TriggerId]Trigger
	streams  map[StreamId]Stream
}

func NewServer(data DataSrc) *Server {
	s := &Server{}

	s.data = data

	s.streams = make(map[StreamId]Stream)

	s.triggers = make(map[TriggerId]Trigger)
	s.triggers[globalTriggerId] = makeTrigger()

	return s
}

func makeTrigger() Trigger {
	t := Trigger{
		channel:     make(chan struct{}),
		fanouts:     make(map[int32]*fanout),
		chNewFanout: make(chan *fanout),
		chRemFanout: make(chan *fanout),
	}
	go t.fanout()
	return t
}

func (s *Server) NewTrigger(context.Context, *rpc.MsgVoid) (*rpc.MsgTrigger, error) {
	resp := &rpc.MsgTrigger{}

	var id TriggerId
	exists := true
	for id == 0 || exists {
		id = TriggerId(rand.Uint32())
		_, exists = s.triggers[id]
	}

	s.triggers[id] = makeTrigger()

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

		trigger.channel <- pull
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
	resp.Id = uint32(id)

	s.streams[id] = Stream{trigger: trigger, signals: req.GetSignals()}

	return resp, nil
}

func (s *Server) Stream(req *rpc.MsgStream, out grpc.ServerStreamingServer[rpc.MsgData]) error {
	stream, ok := s.streams[StreamId(req.GetId())]
	if !ok {
		return fmt.Errorf("%T.Trigger: bad stream id %d", s, req.GetId())
	}

	t := stream.trigger.subscribe()
	defer stream.trigger.unsubscribe(t)

	for {
		_, ok := <-t.channel
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

func (t Trigger) subscribe() *fanout {
	f := t.newFanout()
	t.chNewFanout <- f
	return f
}

func (t Trigger) unsubscribe(f *fanout) {
	t.chRemFanout <- f
}

func (t Trigger) fanout() {
	for {
		select {
		case f := <-t.chNewFanout:
			t.fanouts[f.id] = f
		case f := <-t.chRemFanout:
			delete(t.fanouts, f.id)
			close(f.channel)
		case <-t.channel:
			for _, f := range t.fanouts {
				f.channel <- struct{}{}
			}
		}
	}
}

type fanout struct {
	id      int32
	channel chan struct{}
}

func (t Trigger) newFanout() *fanout {
	var id int32
	exists := true
	for exists {
		id = rand.Int31()
		_, exists = t.fanouts[id]
	}
	f := &fanout{
		id:      id,
		channel: make(chan struct{}, 1),
	}
	return f
}
