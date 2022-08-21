package replication

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/coreos/etcd/storage/storagepb"
	"go.etcd.io/etcd/clientv3"
)

const defaultTimeout = 10 * time.Second

// State is a wrapper around the persistent key-value storage
// used to store information about the replication state.
type State struct {
	cl     *clientv3.Client
	prefix string
}

// NewState initialises the connection to the etcd cluster.
func NewState(addr []string, clusterName string) (*State, error) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   addr,
		DialTimeout: defaultTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("creating etcd client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	_, err = etcdClient.Put(ctx, "test", "test")
	if err != nil {
		return nil, fmt.Errorf("could not set the test key: %w", err)
	}

	return &State{
		cl:     etcdClient,
		prefix: "go-queue/" + clusterName + "/",
	}, nil
}

func (c *State) put(ctx context.Context, key, value string) error {
	_, err := c.cl.Put(ctx, c.prefix+key, value)
	return err
}

type Result struct {
	Key   string
	Value string
}

type Option clientv3.OpOption

func WithPrefix() Option { return Option(clientv3.WithPrefix()) }

func (c *State) get(ctx context.Context, key string, opts ...Option) ([]Result, error) {
	etcdOpts := make([]clientv3.OpOption, 0, len(opts))
	for _, o := range opts {
		etcdOpts = append(etcdOpts, clientv3.OpOption(o))
	}

	etcdRes, err := c.cl.Get(ctx, c.prefix+key, etcdOpts...)
	if err != nil {
		return nil, err
	}

	res := make([]Result, 0, len(etcdRes.Kvs))
	for _, kv := range etcdRes.Kvs {
		res = append(res, Result{
			Key:   string(kv.Key),
			Value: string(kv.Value),
		})
	}

	return res, nil
}

type Peer struct {
	InstanceName string
	ListenAddr   string
}

func (c *State) RegisterNewPeer(ctx context.Context, p Peer) error {
	return c.put(ctx, "peers/"+p.InstanceName, p.ListenAddr)
}

func (c *State) ListPeers(ctx context.Context) ([]Peer, error) {
	resp, err := c.get(ctx, "peers/", WithPrefix())
	if err != nil {
		return nil, err
	}

	res := make([]Peer, 0, len(resp))
	for _, kv := range resp {
		res = append(res, Peer{
			InstanceName: strings.TrimPrefix(kv.Key, c.prefix+"peers/"),
			ListenAddr:   kv.Value,
		})
	}

	return res, nil
}

type Chunk struct {
	Owner    string
	Category string
	FileName string
}

func (c *State) AddChunkToReplicationQueue(ctx context.Context, targetInstance string, ch Chunk) error {
	key := "replication/" + targetInstance + "/" + ch.Category + "/" + ch.FileName
	return c.put(ctx, key, ch.Owner)
}
func (c *State) DeleteChunkFromReplicationQueue(ctx context.Context, targetInstance string, ch Chunk) error {
	key := "replication/" + targetInstance + "/" + ch.Category + "/" + ch.FileName
	log.Printf("prefix %v, key %v", c.prefix, key)
	_, err := c.cl.Delete(ctx, c.prefix+key)
	return err
}

type WatchResponse clientv3.WatchResponse

func (c *State) ParseReplicationKey(prefix string, kv *storagepb.KeyValue) (Chunk, error) {
	log.Printf("state's prefix %s, prefix %s, key %s", c.prefix, prefix, string(kv.Key))
	parts := strings.SplitN(strings.TrimPrefix(string(kv.Key), prefix), "/", 2)
	if len(parts) != 2 {
		return Chunk{}, fmt.Errorf("unexpected key %q, expected two parts after prefix %q",
			string(kv.Key), prefix)
	}
	log.Printf("parts is %v", parts)
	return Chunk{
		Owner:    string(kv.Value),
		Category: parts[0],
		FileName: parts[1],
	}, nil
}

func (c *State) WatchReplicationQueue(ctx context.Context, instanceName string) chan Chunk {
	prefix := c.prefix + "replication/" + instanceName + "/"
	resCh := make(chan Chunk)

	go func() {
		resp, err := c.cl.Get(ctx, prefix, clientv3.WithPrefix())
		// TODO: hande errors better
		if err != nil {
			log.Printf("etcd list keys failed (SOME CHUNKS WILL NOT BE DOWNLOADED): %v", err)
			return
		}

		for _, kv := range resp.Kvs {
			ch, err := c.ParseReplicationKey(prefix, kv)
			if err != nil {
				log.Printf("etcd initial key list error: %v", err)
				continue
			}
			resCh <- ch
		}
	}()

	go func() {
		for resp := range c.cl.Watch(ctx, prefix, clientv3.WithPrefix()) {
			// TODO: hande errors better
			if err := resp.Err(); err != nil {
				log.Printf("etcd watch error: %v", err)
			}
			for _, ev := range resp.Events {
				if len(ev.Kv.Value) == 0 {
					continue
				}
				ch, err := c.ParseReplicationKey(prefix, ev.Kv)
				if err != nil {
					log.Printf("etcd watch error: %v", err)
					continue
				}

				resCh <- ch
			}
		}
	}()
	return resCh
}
