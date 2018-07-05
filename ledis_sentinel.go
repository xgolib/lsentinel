// Package sentinel provides a convenient interface with a redis sentinel which
// will automatically handle pooling connections and automatic failover.
//
// Here's an example of creating a sentinel client and then using it to perform
// some commands
//
//	func example() error {
//		// If there exists sentinel masters "bucket0" and "bucket1", and we want
//		// out client to create pools for both:
//		client, err := sentinel.NewClient("tcp", "localhost:6379", 100)
//		if err != nil {
//			return err
//		}
//
//		if err := exampleCmd(client); err != nil {
//			return err
//		}
//
//		return nil
//	}
//
//	func exampleCmd(client *sentinel.Client) error {
//		conn, err := client.GetMaster()
//		if err != nil {
//			return redisErr
//		}
//		defer client.PutMaster(conn)
//
//		i, err := conn.Cmd("GET", "foo").Int()
//		if err != nil {
//			return err
//		}
//
//		if err := conn.Cmd("SET", "foo", i+1); err != nil {
//			return err
//		}
//
//		return nil
//	}
//
// This package only guarantees that when GetMaster is called the returned
// connection will be a connection to the master as of the moment that method is
// called. It is still possible that there is a failover as that connection is
// being used by the application.
//
// As a final note, a Client can be interacted with from multiple routines at
// once safely, except for the Close method. To safely Close, ensure that only
// one routine ever makes the call and that once the call is made no other
// methods are ever called by any routines.
package lsentinel

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/liuzl/dl"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
)

// ClientError is an error wrapper returned by operations in this package. It
// implements the error interface and can therefore be passed around as a normal
// error.
type ClientError struct {
	err error

	// If this is true the error is due to a problem with the sentinel
	// connection, either it being closed or otherwise unavailable. If false the
	// error is due to some other circumstances. This is useful if you want to
	// implement some kind of reconnecting to sentinel on an error.
	SentinelErr bool
}

var (
	EmptyMasterErr = errors.New("failed to get master from failover service")
)

// Error implements the error protocol
func (ce *ClientError) Error() string {
	return ce.err.Error()
}

type getReqRet struct {
	conn *redis.Client
	err  *ClientError
}

type getReq struct {
	retCh chan *getReqRet
}

type putReq struct {
	conn *redis.Client
}

type switchMaster struct {
	addr string
}

// Client communicates with a sentinel instance and manages connection pools of
// active masters
type Client struct {
	poolSize   int
	masterPool *pool.Pool

	// This is pool.DialFunc instead of the package's DialFunc
	// as it's only used when calling pool.NewCustom. Otherwise it
	// will have to be cast on each invocation.
	dialFunc pool.DialFunc

	getCh   chan *getReq
	putCh   chan *putReq
	closeCh chan struct{}

	clientErr      *ClientError
	clientErrCh    chan *ClientError
	switchMasterCh chan *switchMaster

	master        string
	failoverAddrs []string
}

// DialFunc is a function which can be passed into NewClientCustom
type DialFunc func(network, addr string) (*redis.Client, error)

// NewClient creates a sentinel client. Connects to the given sentinel instance,
// pulls the information for the masters, and creates an
// initial pool of connections for each master. The client will automatically
// replace the pool for any master should sentinel decide to fail the master
// over. The returned error is a *ClientError.
func NewClient(
	network, address string, poolSize int,
) (
	*Client, error,
) {
	return NewClientCustom(network, address, poolSize, redis.Dial)
}

// NewClientCustom is the same as NewClient, except it takes in a DialFunc which
// will be used to create all new connections to the master instances. This can
// be used to implement authentication, custom timeouts, etc...
func NewClientCustom(
	network, address string, poolSize int, df DialFunc,
) (
	*Client, error,
) {
	c := &Client{
		poolSize:       poolSize,
		dialFunc:       (pool.DialFunc)(df),
		getCh:          make(chan *getReq),
		putCh:          make(chan *putReq),
		closeCh:        make(chan struct{}),
		clientErrCh:    make(chan *ClientError),
		switchMasterCh: make(chan *switchMaster),
		failoverAddrs:  strings.Split(address, ";"),
	}

	master := c.masterFromFailover()
	if len(master) == 0 {
		return nil, &ClientError{err: EmptyMasterErr}
	}
	masterPool, err := pool.NewCustom("tcp", master, poolSize, (pool.DialFunc)(df))
	if err != nil {
		return nil, &ClientError{err: err}
	}

	c.masterPool = masterPool
	c.master = master

	go c.subSpin()
	go c.spin()
	return c, nil
}

func (c *Client) masterFromFailover() (master string) {
	var wg sync.WaitGroup
	var l sync.Mutex
	masters := make(map[string]int)
	for idx, _ := range c.failoverAddrs {
		wg.Add(1)
		go func(idx int, masters map[string]int, wg *sync.WaitGroup, l *sync.Mutex) {
			defer wg.Done()
			requestInfo := &dl.HttpRequest{
				Url:      fmt.Sprintf("http://%s/master", c.failoverAddrs[idx]),
				Method:   "GET",
				UseProxy: false,
				Timeout:  2,
			}
			resp := dl.Download(requestInfo)
			if resp.Error != nil {
				return
			}
			l.Lock()
			defer l.Unlock()
			if _, ok := masters[resp.Text]; !ok {
				masters[resp.Text] = 1
			} else {
				masters[resp.Text]++
			}
		}(idx, masters, &wg, &l)
	}
	wg.Wait()
	for node, cnt := range masters {
		if cnt > len(c.failoverAddrs)/2 {
			master = node
			return
		}
	}
	return
}

func (c *Client) subSpin() {
	clientErr := func(err error) {
		e := &ClientError{err: err, SentinelErr: true}
		if err == nil {
			e = nil
		}
		select {
		case c.clientErrCh <- e:
		case <-c.closeCh:
		}
	}

	emptyMasterCount := 0
	for {
		time.Sleep(0.5 * time.Second)
		master := c.masterFromFailover()
		if len(master) == 0 {
			emptyMasterCount++
			if emptyMasterCount > 3 {
				clientErr(EmptyMasterErr)
				continue
			}
		}
		if emptyMasterCount > 0 {
			emptyMasterCount = 0
			clientErr(nil)
		}
		if master == c.master {
			continue
		}
		c.master = master
		select {
		case c.switchMasterCh <- &switchMaster{master}:
		case <-c.closeCh:
			return
		}
	}
}

func (c *Client) spin() {
	for {
		select {
		case req := <-c.getCh:
			if c.clientErr != nil {
				req.retCh <- &getReqRet{nil, c.clientErr}
				continue
			}
			conn, err := c.masterPool.Get()
			if err != nil {
				req.retCh <- &getReqRet{nil, &ClientError{err: err}}
				continue
			}
			req.retCh <- &getReqRet{conn, nil}

		case req := <-c.putCh:
			c.masterPool.Put(req.conn)

		case err := <-c.clientErrCh:
			c.clientErr = err

		case sm := <-c.switchMasterCh:
			c.masterPool.Empty()
			c.masterPool, _ = pool.NewCustom("tcp", sm.addr, c.poolSize, c.dialFunc)

		case <-c.closeCh:
			c.masterPool.Empty()
			close(c.getCh)
			close(c.putCh)
			return
		}
	}
}

// GetMaster retrieves a connection for the master. If
// sentinel has become unreachable this will client return an error. Close
// should be called in that case. The returned error is a *ClientError.
func (c *Client) GetMaster() (*redis.Client, error) {
	req := getReq{make(chan *getReqRet)}
	c.getCh <- &req
	ret := <-req.retCh
	if ret.err != nil {
		return nil, ret.err
	}
	return ret.conn, nil
}

// PutMaster return a connection for a master
func (c *Client) PutMaster(client *redis.Client) {
	c.putCh <- &putReq{client}
}

// Close closes all connection pools as well as the connection to sentinel.
func (c *Client) Close() {
	close(c.closeCh)
}
