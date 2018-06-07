package udpchannel

import (
	"errors"
	"net"
	"sync"
)

var (
	ErrClosed          = errors.New("udpchannel closed")
	ErrZeroSized       = errors.New("zero sized message")
	ErrMessageTooLarge = errors.New("message too large")
)

type Client struct {
	c          *net.UDPConn
	bufferChan chan msg
	closeChan  chan struct{}
	wg         sync.WaitGroup
	closed     bool
	m          sync.Mutex
}

type msg struct {
	addr *net.UDPAddr
	data []byte
}

func NewClient() (*Client, error) {
	var err error
	client := &Client{
		bufferChan: make(chan msg, 1000),
		closeChan:  make(chan struct{}, 0),
	}

	client.c, err = net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}

	client.wg.Add(1)
	go func() {
		client.run()
	}()

	return client, nil
}

func (c *Client) run() {
	defer c.wg.Done()
	for {
		select {
		case <-c.closeChan:
			return
		case m := <-c.bufferChan:
			c.c.WriteToUDP(m.data, m.addr)
			// TODO: Handle error/invalid len
		}
	}
}

func (c *Client) Close() error {
	close(c.closeChan)

	c.wg.Wait()

	close(c.bufferChan)

	err := c.c.Close()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Send(raddr string, data []byte) error {
	dataLen := len(data)
	if dataLen == 0 {
		return ErrZeroSized
	}

	if dataLen > 1500 {
		return ErrMessageTooLarge
	}

	addr, err := net.ResolveUDPAddr("udp", raddr)
	if err != nil {
		return err
	}

	c.m.Lock()
	if c.closed {
		c.m.Unlock()
		return ErrClosed
	}
	c.wg.Add(1)
	c.m.Unlock()
	defer c.wg.Done()

	c.bufferChan <- msg{
		addr: addr,
		data: data,
	}

	return nil
}
