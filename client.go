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

func (c *Client) NewStream(signalNames []string, triggerId TriggerId) (StreamId, error) {

	resp, err := c.rpc.NewStream(context.Background(),
		&rpc.MsgStreamReq{
			TriggerId: uint32(triggerId),
			Signals:   signalNames,
		},
	)

	if err != nil {
		return 0, fmt.Errorf("%T.NewStream: %w", c, err)
	}

	return StreamId(resp.GetId()), nil
}

func (c *Client) NewStreamWithGlobalTrigger(signalNames []string) (StreamId, error) {
	return c.NewStream(signalNames, globalTriggerId)
}

type trigger struct {
	client *Client

	id TriggerId

	trig Trigger
}

func (c *Client) initGlobalTrigger() error {
	if c.globalTrigger != nil {
		return nil
	}

	t := &trigger{
		client: c,
		id:     globalTriggerId,
		trig:   make(Trigger),
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

func (c *Client) NewTrigger() (*trigger, error) {

	resp, err := c.rpc.NewTrigger(context.Background(), &rpc.MsgVoid{})
	if err != nil {
		return nil, fmt.Errorf("%T.NewTrigger: gRPC error: %w", c, err)
	}

	id := TriggerId(resp.GetId())

	t := &trigger{
		client: c,
		id:     id,
		trig:   make(Trigger),
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

	triggerPipe, err := t.client.rpc.Trigger(context.Background())
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
