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

	triggers map[TriggerId]Trigger

	globalTriggerReady bool
}

func NewClient() *Client {
	c := &Client{}

	c.triggers = make(map[TriggerId]Trigger)

	return c
}

func (c *Client) Connect(hostname string, port int) error {

	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", hostname, port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("%T.Connect: could not get grpc client: %w", c, err)
	}

	c.rpc = rpc.NewTriggeredStreamClient(conn)

	return nil
}

func (c *Client) NewTrigger() (TriggerId, error) {

	resp, err := c.rpc.NewTrigger(context.Background(), &rpc.MsgVoid{})
	if err != nil {
		return 0, fmt.Errorf("%T.NewTrigger: gRPC error: %w", c, err)
	}

	id := TriggerId(resp.GetId())

	err = c.startTriggerMan(id)
	if err != nil {
		return 0, fmt.Errorf("%T.Client: could not start the trigger manager: %w", c, err)
	}

	c.triggers[id] = make(Trigger)
	return id, nil
}

func (c *Client) Trigger(id TriggerId) error {

	trigger, ok := c.triggers[id]
	if !ok {
		return fmt.Errorf("%T.Client: no trigger with id %d", c, id)
	}

	trigger <- pull

	return nil
}

func (c *Client) initGlobalTrigger() error {
	if c.globalTriggerReady {
		return nil
	}

	err := c.startTriggerMan(globalTriggerId)
	if err != nil {
		return fmt.Errorf("%T.Client: could not start the global trigger manager: %w", c, err)
	}

	c.triggers[globalTriggerId] = make(Trigger)

	c.globalTriggerReady = true
	return nil
}

func (c *Client) GlobalTrigger() error {
	err := c.initGlobalTrigger()
	if err != nil {
		return fmt.Errorf("%T.GlobalTrigger: %w", c, err)
	}
	return c.Trigger(globalTriggerId)
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

func (c *Client) startTriggerMan(id TriggerId) error {

	trigger, ok := c.triggers[id]
	if !ok {
		return fmt.Errorf("%T.Client: no trigger with id %d", c, id)
	}

	triggerPipe, err := c.rpc.Trigger(context.Background())
	if err != nil {
		return fmt.Errorf("%T.Client: could not open the trigger pipe: %w", c, err)
	}

	go func() {
		// for ever loop exits when trigger channel is closed
		// or on gRPC send error
		for {
			_, ok := <-trigger
			if !ok {
				return
			}

			err := triggerPipe.Send(&rpc.MsgTrigger{Id: uint32(id)})
			if err != nil {
				return
			}
		}
	}()

	return nil
}
