// Package sniff is a Kafka proxy that reads every request passing through
// it. Clients bootstrap from the proxy; it rewrites the broker addresses in
// the answers that carry them (Metadata, FindCoordinator, DescribeCluster,
// and the leader hints on Produce and Fetch), so every later connection
// comes through a listener of its own as well. The bytes it forwards are
// otherwise the client's and the broker's own.
package sniff

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Request is one request and, once it came, its response.
type Request struct {
	N        int64 // in the order the proxy read them, from 1
	At       time.Time
	Client   string // the client's address
	ClientID string
	Broker   int32
	Key      int16
	Version  int16
	Size     int // bytes on the wire, size prefix included
	RespSize int // 0 when no response is due
	Took     time.Duration
	Answered bool   // false for a produce with acks=0, which gets no response
	Info     string // what the request asks for, decoded
	Rewrote  bool   // its response's broker addresses now point at the proxy
	Head     []byte // the frame's first bytes, for a hex dump
}

// Name is the request's API name, such as Produce.
func (r Request) Name() string { return kmsg.NameForKey(r.Key) }

// Route is one broker and the listener that stands in for it.
type Route struct {
	Broker   int32
	Listen   string
	Upstream string
}

// headBytes is how much of each frame Request.Head keeps.
const headBytes = 64

// maxFrame bounds a frame the proxy will read: Kafka's own default
// socket.request.max.bytes is 100 MiB.
const maxFrame = 100 << 20

// Proxy is a running sniffer.
type Proxy struct {
	host string
	port int // the next port to listen on; 0 for any
	out  func(Request)
	ctx  context.Context
	stop context.CancelFunc
	n    atomic.Int64

	mu     sync.Mutex
	routes map[int32]*Route
	lns    []net.Listener
	conns  map[net.Conn]bool
	names  map[[16]byte]string // topic IDs, learned from Metadata answers
	first  int32
	wg     sync.WaitGroup
}

// Start asks cl for the cluster's brokers and listens for each, the first
// by ID on listen and the rest on the ports after it (or any port, when
// listen's is 0). out is called for every request once it is answered,
// from many goroutines at once.
func Start(ctx context.Context, cl *kgo.Client, listen string, out func(Request)) (*Proxy, error) {
	host, ps, err := net.SplitHostPort(listen)
	if err != nil {
		return nil, fmt.Errorf("listen address %q: %w", listen, err)
	}
	port, err := strconv.Atoi(ps)
	if err != nil || port < 0 || port > 65535 {
		return nil, fmt.Errorf("listen address %q: the port must be a number from 0 to 65535", listen)
	}
	resp, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	if len(resp.Brokers) == 0 {
		return nil, errors.New("the cluster named no brokers")
	}
	pctx, stop := context.WithCancel(context.Background())
	p := &Proxy{host: host, port: port, out: out, ctx: pctx, stop: stop,
		routes: map[int32]*Route{}, conns: map[net.Conn]bool{}, names: map[[16]byte]string{}}
	p.learn(resp)
	bs := resp.Brokers
	sort.Slice(bs, func(i, j int) bool { return bs[i].NodeID < bs[j].NodeID })
	p.first = bs[0].NodeID
	for _, b := range bs {
		if _, err := p.route(b.NodeID, net.JoinHostPort(b.Host, strconv.Itoa(int(b.Port)))); err != nil {
			p.Close()
			return nil, err
		}
	}
	return p, nil
}

// Addr is where clients bootstrap from.
func (p *Proxy) Addr() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.routes[p.first].Listen
}

// Routes are the brokers and their listeners, by broker ID.
func (p *Proxy) Routes() []Route {
	p.mu.Lock()
	defer p.mu.Unlock()
	var rs []Route
	for _, r := range p.routes {
		rs = append(rs, *r)
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].Broker < rs[j].Broker })
	return rs
}

// Close stops listening, ends every proxied connection, and waits for them.
func (p *Proxy) Close() {
	p.stop()
	p.mu.Lock()
	for _, l := range p.lns {
		_ = l.Close()
	}
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

// route is the listener for broker id, made on first sight: a broker can
// join after the proxy starts, and its first mention is an answer to
// rewrite.
func (p *Proxy) route(id int32, upstream string) (*Route, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if r, ok := p.routes[id]; ok {
		if r.Upstream != upstream && upstream != "" {
			r.Upstream = upstream // the broker moved
		}
		return r, nil
	}
	if p.ctx.Err() != nil {
		return nil, errors.New("the proxy is closed")
	}
	addr := net.JoinHostPort(p.host, strconv.Itoa(p.port))
	l, err := net.Listen("tcp", addr)
	if err != nil {
		if p.port == 0 || len(p.routes) == 0 {
			return nil, err // the port asked for, or any port: nothing to fall back to
		}
		if l, err = net.Listen("tcp", net.JoinHostPort(p.host, "0")); err != nil {
			return nil, err
		}
	}
	if p.port != 0 {
		p.port++
	}
	r := &Route{Broker: id, Listen: l.Addr().String(), Upstream: upstream}
	if p.host != "" && p.host != "0.0.0.0" && p.host != "::" {
		// Answer with the name the client used, not the address it resolved to.
		_, lp, _ := net.SplitHostPort(r.Listen)
		r.Listen = net.JoinHostPort(p.host, lp)
	}
	p.routes[id] = r
	p.lns = append(p.lns, l)
	p.wg.Add(1)
	go p.serve(l, id)
	return r, nil
}

func (p *Proxy) serve(l net.Listener, id int32) {
	defer p.wg.Done()
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.ctx.Err() != nil {
			p.mu.Unlock()
			_ = c.Close()
			return
		}
		up := p.routes[id].Upstream
		p.conns[c] = true
		p.wg.Add(1)
		p.mu.Unlock()
		go p.pipe(c, id, up)
	}
}

// pipe carries one client connection to its broker and back.
func (p *Proxy) pipe(c net.Conn, id int32, upstream string) {
	defer p.wg.Done()
	defer func() {
		p.mu.Lock()
		delete(p.conns, c)
		p.mu.Unlock()
		_ = c.Close()
	}()
	d := net.Dialer{Timeout: 10 * time.Second}
	up, err := d.DialContext(p.ctx, "tcp", upstream)
	if err != nil {
		return // the client sees its connection close, as it would have
	}
	p.mu.Lock()
	p.conns[up] = true
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.conns, up)
		p.mu.Unlock()
		_ = up.Close()
	}()

	var mu sync.Mutex
	pending := map[int32]*Request{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = up.Close(); _ = c.Close() }()
		for {
			frame, err := readFrame(up)
			if err != nil {
				return
			}
			if len(frame) < 8 {
				return
			}
			corr := int32(binary.BigEndian.Uint32(frame[4:]))
			mu.Lock()
			r := pending[corr]
			delete(pending, corr)
			mu.Unlock()
			if r != nil {
				frame = p.answer(r, frame)
				r.RespSize, r.Took, r.Answered = len(frame), time.Since(r.At), true
			}
			if _, err := c.Write(frame); err != nil {
				return
			}
			if r != nil && p.out != nil {
				p.out(*r)
			}
		}
	}()
	client := c.RemoteAddr().String()
	for {
		frame, err := readFrame(c)
		if err != nil {
			break
		}
		r, corr, ok := p.request(frame, client, id)
		waits := ok && r.Answered
		if waits {
			cp := r // the response side owns this copy now
			mu.Lock()
			pending[corr] = &cp
			mu.Unlock()
		}
		if _, err := up.Write(frame); err != nil {
			break
		}
		if ok && !waits && p.out != nil {
			p.out(r) // acks=0: nothing will come back
		}
	}
	_ = up.Close()
	<-done
}

// readFrame reads one size-prefixed frame, prefix included.
func readFrame(r io.Reader) ([]byte, error) {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return nil, err
	}
	n := int32(binary.BigEndian.Uint32(size[:]))
	if n < 0 || n > maxFrame {
		return nil, fmt.Errorf("a frame of %d bytes", n)
	}
	frame := make([]byte, 4+int(n))
	copy(frame, size[:])
	if _, err := io.ReadFull(r, frame[4:]); err != nil {
		return nil, err
	}
	return frame, nil
}

// skipTags steps over a tagged-fields section.
func skipTags(b []byte) ([]byte, bool) {
	n, k := binary.Uvarint(b)
	if k <= 0 {
		return nil, false
	}
	b = b[k:]
	for range n {
		if _, k = binary.Uvarint(b); k <= 0 {
			return nil, false
		}
		b = b[k:]
		size, k := binary.Uvarint(b)
		if k <= 0 || uint64(len(b)-k) < size {
			return nil, false
		}
		b = b[k+int(size):]
	}
	return b, true
}

// request reads a request's header and decodes what it asks for. ok is
// false for a frame too short to be a request; it is forwarded all the same.
func (p *Proxy) request(frame []byte, client string, broker int32) (Request, int32, bool) {
	if len(frame) < 12 {
		return Request{}, 0, false
	}
	b := frame[4:]
	r := Request{
		N: p.n.Add(1), At: time.Now(), Client: client, Broker: broker,
		Key: int16(binary.BigEndian.Uint16(b)), Version: int16(binary.BigEndian.Uint16(b[2:])),
		Size: len(frame), Answered: true,
	}
	r.Head = append([]byte(nil), frame[:min(len(frame), headBytes)]...)
	corr := int32(binary.BigEndian.Uint32(b[4:]))
	b = b[8:]
	if len(b) >= 2 {
		n := int16(binary.BigEndian.Uint16(b))
		b = b[2:]
		if n > 0 && int(n) <= len(b) {
			r.ClientID, b = string(b[:n]), b[n:]
		}
	}
	req := kmsg.RequestForKey(r.Key)
	if req == nil {
		return r, corr, true
	}
	req.SetVersion(r.Version)
	if req.IsFlexible() {
		var ok bool
		if b, ok = skipTags(b); !ok {
			return r, corr, true
		}
	}
	if err := req.ReadFrom(b); err != nil {
		return r, corr, true
	}
	if pr, ok := req.(*kmsg.ProduceRequest); ok && pr.Acks == 0 {
		r.Answered = false
	}
	r.Info = p.describe(req)
	return r, corr, true
}

// answer learns topic names from a response and points the broker
// addresses it carries at the proxy. Anything it can't decode passes as
// it came.
func (p *Proxy) answer(r *Request, frame []byte) []byte {
	switch r.Key {
	case 0, 1, 3, 10, 60:
	default:
		return frame
	}
	if (r.Key == 0 && r.Version < 10) || (r.Key == 1 && r.Version < 16) {
		return frame // no broker addresses before these versions
	}
	resp := kmsg.ResponseForKey(r.Key)
	resp.SetVersion(r.Version)
	head := frame[4:8]
	body := frame[8:]
	if resp.IsFlexible() {
		rest, ok := skipTags(body)
		if !ok {
			return frame
		}
		head, body = frame[4:len(frame)-len(rest)], rest
	}
	if err := resp.ReadFrom(body); err != nil {
		return frame
	}
	changed := false
	point := func(id int32, host *string, port *int32) {
		rt, err := p.route(id, net.JoinHostPort(*host, strconv.Itoa(int(*port))))
		if err != nil {
			return
		}
		h, ps, _ := net.SplitHostPort(rt.Listen)
		pn, _ := strconv.Atoi(ps)
		*host, *port, changed = h, int32(pn), true
	}
	switch resp := resp.(type) {
	case *kmsg.ProduceResponse:
		for i := range resp.Brokers {
			b := &resp.Brokers[i]
			point(b.NodeID, &b.Host, &b.Port)
		}
	case *kmsg.FetchResponse:
		for i := range resp.Brokers {
			b := &resp.Brokers[i]
			point(b.NodeID, &b.Host, &b.Port)
		}
	case *kmsg.MetadataResponse:
		p.learn(resp)
		for i := range resp.Brokers {
			b := &resp.Brokers[i]
			point(b.NodeID, &b.Host, &b.Port)
		}
	case *kmsg.FindCoordinatorResponse:
		if resp.Host != "" {
			point(resp.NodeID, &resp.Host, &resp.Port)
		}
		for i := range resp.Coordinators {
			c := &resp.Coordinators[i]
			if c.Host != "" {
				point(c.NodeID, &c.Host, &c.Port)
			}
		}
	case *kmsg.DescribeClusterResponse:
		for i := range resp.Brokers {
			b := &resp.Brokers[i]
			point(b.NodeID, &b.Host, &b.Port)
		}
	}
	if !changed {
		return frame
	}
	r.Rewrote = true
	out := make([]byte, 4, len(frame)+64)
	out = append(out, head...)
	out = resp.AppendTo(out)
	binary.BigEndian.PutUint32(out, uint32(len(out)-4))
	return out
}

// learn keeps the topic IDs a Metadata answer names.
func (p *Proxy) learn(resp *kmsg.MetadataResponse) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, t := range resp.Topics {
		if t.Topic != nil && t.TopicID != [16]byte{} {
			p.names[t.TopicID] = *t.Topic
		}
	}
}

// topic is a topic's name, by name or by ID.
func (p *Proxy) topic(name string, id [16]byte) string {
	if name != "" {
		return name
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if n, ok := p.names[id]; ok {
		return n
	}
	return fmt.Sprintf("%x…", id[:4])
}
