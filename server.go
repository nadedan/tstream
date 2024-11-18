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
type fanoutId uint32

type serverStream struct {
	trigger serverTrigger
	signals []string
}

type TriggeredStreamService struct {
	rpc.UnimplementedTriggeredStreamServer

	data DataSrc

	triggers map[TriggerId]serverTrigger
	streams  map[StreamId]serverStream
}

func NewTriggeredStreamService(data DataSrc) *TriggeredStreamService {
	s := &TriggeredStreamService{}

	s.data = data

	s.streams = make(map[StreamId]serverStream)

	s.triggers = make(map[TriggerId]serverTrigger)
	s.triggers[globalTriggerId] = makeServerTrigger()

	return s
}

// NewTrigger generates a random, unique, id to be used for triggering subscribed streams
func (s *TriggeredStreamService) NewTrigger(context.Context, *rpc.MsgVoid) (*rpc.MsgTrigger, error) {
	resp := &rpc.MsgTrigger{}

	var id TriggerId
	exists := true
	for id == 0 || exists {
		id = TriggerId(rand.Uint32())
		_, exists = s.triggers[id]
	}

	s.triggers[id] = makeServerTrigger()

	resp.Id = uint32(id)

	return resp, nil
}

// Trigger all streams, registered to the given trigger id, to send data to their clients
func (s *TriggeredStreamService) Trigger(in grpc.ClientStreamingServer[rpc.MsgTrigger, rpc.MsgVoid]) error {

	for {
		msg, err := in.Recv()
		if err != nil {
			return fmt.Errorf("%T.Trigger: %w", s, err)
		}

		trigger, ok := s.triggers[TriggerId(msg.GetId())]
		if !ok {
			return fmt.Errorf("%T.Trigger: bad trigger id %d", s, msg.GetId())
		}

		trigger.channel <- struct{}{}
	}
}

// NewStream allows a client to assign a stream request to a trigger id. The client will then receive new data every time
// the requeted trigger is pulled
func (s *TriggeredStreamService) NewStream(ctx context.Context, req *rpc.MsgStreamReq) (*rpc.MsgStream, error) {
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

	s.streams[id] = serverStream{trigger: trigger, signals: req.GetSignals()}

	return resp, nil
}

// Stream sends data to clients when the corresponding stream trigger is pulled
func (s *TriggeredStreamService) Stream(req *rpc.MsgStream, out grpc.ServerStreamingServer[rpc.MsgData]) error {
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

type serverTrigger struct {
	channel     chan struct{}
	fanouts     map[fanoutId]*fanout
	chNewFanout chan *fanout
	chRemFanout chan *fanout
}

// makeServerTrigger creates and inits a serverTrigger and starts its fanout go routine
func makeServerTrigger() serverTrigger {
	t := serverTrigger{
		channel:     make(chan struct{}),
		fanouts:     make(map[fanoutId]*fanout),
		chNewFanout: make(chan *fanout),
		chRemFanout: make(chan *fanout),
	}
	go t.fanout()
	return t
}

func (t serverTrigger) subscribe() *fanout {
	f := t.newFanout()
	t.chNewFanout <- f
	return f
}

func (t serverTrigger) unsubscribe(f *fanout) {
	t.chRemFanout <- f
}

func (t serverTrigger) fanout() {
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
	id      fanoutId
	channel chan struct{}
}

func (t serverTrigger) newFanout() *fanout {
	var id fanoutId
	exists := true
	for exists {
		id = fanoutId(rand.Int31())
		_, exists = t.fanouts[id]
	}
	f := &fanout{
		id:      id,
		channel: make(chan struct{}, 1),
	}
	return f
}
