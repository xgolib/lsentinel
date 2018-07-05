package lsentinel

import (
	"crypto/rand"
	"encoding/hex"
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests assume there is a master/slave running on ports 8000/7001, and a
// sentinel which tracks them under the name "test" on port 28000
//
// You can use `make start` to automatically set these up.

func getSentinel(t *T) *Client {
	s, err := NewClient("tcp", "127.0.0.1:11000;127.0.0.1:11001;127.0.0.1:11002", 10)
	require.Nil(t, err)
	return s
}

func randStr() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func TestBasic(t *T) {
	s := getSentinel(t)
	k := randStr()

	c, err := s.GetMaster()
	require.Nil(t, err)
	require.Nil(t, c.Cmd("SET", k, "foo").Err)
	s.PutMaster(c)

	c, err = s.GetMaster()
	require.Nil(t, err)
	foo, err := c.Cmd("GET", k).Str()
	require.Nil(t, err)
	assert.Equal(t, "foo", foo)
	s.PutMaster(c)
}

// Test a basic manual failover
func TestFailover(t *T) {
	s := getSentinel(t)

	k := randStr()

	c, err := s.GetMaster()
	require.Nil(t, err)
	require.Nil(t, c.Cmd("SET", k, "foo").Err)
	s.PutMaster(c)

	t.Log("stop master")
	time.Sleep(20 * time.Second)
	c, err = s.GetMaster()
	require.Nil(t, err)
	foo, err := c.Cmd("GET", k).Str()
	require.Nil(t, err)
	assert.Equal(t, "foo", foo)
	require.Nil(t, c.Cmd("SET", k, "bar").Err)
	s.PutMaster(c)
}
