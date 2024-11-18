package tstream

import (
	"context"
	"fmt"

	"tstream/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	rpc rpc.TriggeredStreamClient

	globalTrigger *trigger
}

func NewClient() *Client {
	c := &Client{}

	return c
}

func (c *Client) Connect(hostname string, port int) error {

	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", hostname, port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("%T.Connect: could not get grpc client: %w", c, err)
	}

	c.rpc = rpc.NewTriggeredStreamClient(conn)

	err = c.initGlobalTrigger()
	if err != nil {
		return fmt.Errorf("%T.Connect: %w", c, err)
	}

	return nil
}

type stream struct {
	c      *Client
	id     StreamId
	pipe   chan []uint32
	stream grpc.ServerStreamingClient[rpc.MsgData]
}

func (c *Client) NewStream(signalNames []string, triggerId TriggerId) (*stream, error) {

	resp, err := c.rpc.NewStream(context.Background(),
		&rpc.MsgStreamReq{
			TriggerId: uint32(triggerId),
			Signals:   signalNames,
		},
	)

	if err != nil {
		return nil, fmt.Errorf("%T.NewStream: %w", c, err)
	}

	s := &stream{
		c:    c,
		id:   StreamId(resp.GetId()),
		pipe: make(chan []uint32),
	}

	err = s.start()
	if err != nil {
		close(s.pipe)
		return nil, fmt.Errorf("%T.NewStream: could not start stream: %w", c, err)
	}

	return s, nil
}

func (s *stream) start() error {
	var err error
	s.stream, err = s.c.rpc.Stream(context.Background(), &rpc.MsgStream{Id: uint32(s.id)})
	if err != nil {
		return fmt.Errorf("%T.start: could not get the stream: %w", s, err)
	}

	return nil
}

func (s *stream) Recv() (*rpc.MsgData, error) {
	msg, err := s.stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("%T.Recv: %w", s, err)
	}

	return msg, nil
}

func (c *Client) NewStreamWithGlobalTrigger(signalNames []string) (*stream, error) {
	return c.NewStream(signalNames, globalTriggerId)
}

func (c *Client) initGlobalTrigger() error {
	if c.globalTrigger != nil {
		return nil
	}

	t := &trigger{
		c:    c,
		id:   globalTriggerId,
		trig: make(chan struct{}),
	}

	err := t.start()
	if err != nil {
		return fmt.Errorf("%T.initGlobalTrigger: could not start the global trigger manager: %w", c, err)
	}

	c.globalTrigger = t
	return nil
}

func (c *Client) PullGlobalTrigger() {
	c.globalTrigger.Pull()
}

type trigger struct {
	c *Client

	id TriggerId

	trig chan struct{}
}

func (c *Client) NewTrigger() (*trigger, error) {

	resp, err := c.rpc.NewTrigger(context.Background(), &rpc.MsgVoid{})
	if err != nil {
		return nil, fmt.Errorf("%T.NewTrigger: gRPC error: %w", c, err)
	}

	id := TriggerId(resp.GetId())

	t := &trigger{
		c:    c,
		id:   id,
		trig: make(chan struct{}),
	}

	err = t.start()
	if err != nil {
		return nil, fmt.Errorf("%T.NewTrigger: could not start the trigger manager: %w", c, err)
	}

	return t, nil
}

func (t *trigger) Pull() {
	t.trig <- pull
}

func (t *trigger) start() error {

	trigger := t.trig

	triggerPipe, err := t.c.rpc.Trigger(context.Background())
	if err != nil {
		return fmt.Errorf("%T.start: could not open the trigger pipe: %w", t, err)
	}

	go func() {
		// for ever loop exits when trigger channel is closed
		// or on gRPC send error
		for {
			_, ok := <-trigger
			if !ok {
				return
			}

			err := triggerPipe.Send(&rpc.MsgTrigger{Id: uint32(t.id)})
			if err != nil {
				return
			}
		}
	}()

	return nil
}
