package etlmetrics

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/stdiopt/danda/etl"
	"github.com/stdiopt/danda/util/humanize"
)

type perIter struct {
	name     string
	count    uint64
	oldCount uint64
	units    map[string]uint64
	done     bool
}

type Metrics struct {
	mu       sync.Mutex
	perIter  []*perIter
	refs     map[string]*perIter
	mark     time.Time
	interval time.Duration
	quit     chan struct{}
}

type metricOptions func(m *Metrics)

func WithInterval(d time.Duration) metricOptions {
	return func(m *Metrics) {
		m.interval = d
	}
}

func New(opts ...metricOptions) *Metrics {
	m := &Metrics{
		mark:     time.Now(),
		interval: 5 * time.Second,
		quit:     make(chan struct{}),
	}
	for _, fn := range opts {
		fn(m)
	}
	return m
}

func (m *Metrics) create(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.refs == nil {
		m.refs = map[string]*perIter{}
	}
	if m.refs[name] != nil {
		return
	}

	pi := &perIter{
		name:  name,
		units: map[string]uint64{},
	}
	m.perIter = append(m.perIter, pi)
	m.refs[name] = pi
}

func (m *Metrics) add(name string, v any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mm := m.refs[name]
	mm.count++
	mm.units[fmt.Sprintf("%T", v)]++

	/*
		now := time.Now()
		if now.Sub(g.mark) < 5*time.Second {
			return
		}
		secs := time.Since(g.mark).Seconds()
		log.Printf("--- Metrics %.2fs ---", secs)
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		log.Printf("  Mem: %s", humanize.Bytes(mem.Alloc))
		for _, m := range g.perIter {
			diff := m.count - m.oldCount
			m.oldCount = m.count
			log.Printf("  [%s] Processed: %s (%s/s) closed: %v",
				m.name,
				humanize.Number(uint64(m.count)),
				humanize.Number(float64(diff)/secs),
				m.done,
			)

		}
		g.mark = now
	*/
}

func (m *Metrics) done(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mm, ok := m.refs[name]
	if !ok {
		return
	}
	mm.done = true
	/*
		running := 0
		for _, m := range g.perIter {
			if !m.done {
				running++
			}
		}
		if running == 0 {
			log.Printf("--- DONE Metrics ---")
			for _, m := range g.perIter {
				log.Printf("  [%s] Total processed: %d", m.name, m.count)
				for k, v := range m.units {
					log.Printf("  [%s]   %s: %d", m.name, k, v)
				}
			}
		}*/
}

func (m *Metrics) Start() {
	go m.spin()
}

func (m *Metrics) Done() {
	m.mu.Lock()
	defer m.mu.Unlock()
	close(m.quit)
	log.Printf("--- DONE Metrics ---")
	for _, m := range m.perIter {
		log.Printf("  [%s] Total processed: %d closed: %v", m.name, m.count, m.done)
		for k, v := range m.units {
			log.Printf("  [%s]   %s: %d", m.name, k, v)
		}
	}
}

func (m *Metrics) spin() {
	mark := time.Now()
	for {
		select {
		case <-m.quit:
			return
		default:
		}
		time.Sleep(m.interval)
		//if time.Since(mark) < 5*time.Second {
		//	continue
		//}
		running := 0
		func() {
			m.mu.Lock()
			defer m.mu.Unlock()

			secs := time.Since(mark).Seconds()
			log.Printf("--- Metrics %.2fs ---", secs)

			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			log.Printf("  Mem: %s", humanize.Bytes(mem.Alloc))

			for _, m := range m.perIter {
				if !m.done {
					running++
				}
				diff := m.count - m.oldCount
				m.oldCount = m.count
				log.Printf("  [%s] Processed: %s (%s/s) closed: %v",
					m.name,
					humanize.Number(uint64(m.count)),
					humanize.Number(float64(diff)/secs),
					m.done,
				)

			}
			mark = time.Now()
		}()
	}
}

func (m *Metrics) Count(it Iter, name string) Iter {
	m.create(name)
	return etl.MakeIter(etl.Custom[any]{
		Next: func(ctx context.Context) (any, error) {
			v, err := it.Next(ctx)
			m.add(name, v)
			return v, err
		},
		Close: func() error {
			m.done(name)
			return it.Close()
		},
	})
}
