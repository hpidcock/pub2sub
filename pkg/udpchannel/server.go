package udpchannel

import (
	"net"
	"runtime"
	"sync"
)

type Server struct {
	c          *net.UDPConn
	handler    PacketHandler
	bufferChan chan []byte
	closeChan  chan struct{}
	wg         sync.WaitGroup
}

type PacketHandler func([]byte)

func NewServer(laddr string, handler PacketHandler) (*Server, error) {
	server := &Server{
		bufferChan: make(chan []byte, 1000),
		closeChan:  make(chan struct{}, 0),
		handler:    handler,
	}

	addr, err := net.ResolveUDPAddr("udp", laddr)
	if err != nil {
		return nil, err
	}

	server.c, err = net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	server.wg.Add(1)
	go func() {
		server.recv()
	}()

	for i := 0; i < runtime.NumCPU(); i++ {
		server.wg.Add(1)
		go func() {
			server.worker()
		}()
	}

	return server, nil
}

func (s *Server) Close() error {
	close(s.closeChan)

	err := s.c.Close()
	if err != nil {
		return err
	}

	s.wg.Wait()
	close(s.bufferChan)

	return nil
}

func (s *Server) recv() {
	defer s.wg.Done()
	buffer := make([]byte, 1500)
	for {
		select {
		case <-s.closeChan:
			return
		default:
		}

		n, err := s.c.Read(buffer)
		if err != nil {
			return
		}

		if n == 0 {
			continue
		}

		nb := make([]byte, n)
		copy(nb, buffer[:n-1])
		s.bufferChan <- nb
	}
}

func (s *Server) worker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.closeChan:
			return
		case buffer := <-s.bufferChan:
			s.handler(buffer)
		}
	}
}
