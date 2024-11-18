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

// NewClient returns a pointer to a triggered stream client
func NewClient() *Client {
	c := &Client{}

	return c
}

// Connect to a triggered stream server at hostname:port
//
// Inits the global trigger
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

// PullGlobalTrigger to cause the server to Send new data on all streams on the global trigger
func (c *Client) PullGlobalTrigger() {
	c.globalTrigger.Pull()
}

// NewStreamWithGlobalTrigger requests a stream from the server that gets data on every pull of the global trigger
func (c *Client) NewStreamWithGlobalTrigger(signalNames []string) (*stream, error) {
	return c.NewStream(signalNames, globalTriggerId)
}

// NewSteam requests a stream from the server that gets data on every pull of the specified trigger
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
		c:  c,
		id: StreamId(resp.GetId()),
	}

	s.stream, err = s.c.rpc.Stream(context.Background(), &rpc.MsgStream{Id: uint32(s.id)})
	if err != nil {
		return nil, fmt.Errorf("%T.NewStream: could not start stream: %w", c, err)
	}

	return s, nil
}

// NewTrigger gets a new, unique, trigger id from the triggered stream server. This trigger id can be used when setting
// up a NewStream.
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

type stream struct {
	c      *Client
	id     StreamId
	stream grpc.ServerStreamingClient[rpc.MsgData]
}

// Recv blocks until it receives data from the stream
func (s *stream) Recv() (*rpc.MsgData, error) {
	msg, err := s.stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("%T.Recv: %w", s, err)
	}

	return msg, nil
}

// initGlobalTrigger starts the global trigger manager for the triggered stream client
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

type trigger struct {
	c *Client

	id TriggerId

	trig chan struct{}
}

// Pull causes the trigger to fire which will make the stream server send data to all clients on this trigger
func (t *trigger) Pull() {
	t.trig <- pull
}

// start listening for trigger pulls so we can notify the triggered stream server
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
