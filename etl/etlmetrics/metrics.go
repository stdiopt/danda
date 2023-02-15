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

type Grabber struct {
	mu      sync.Mutex
	perIter []*perIter
	refs    map[string]*perIter
	mark    time.Time
	quit    chan struct{}
}

func (g *Grabber) create(name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.refs == nil {
		g.refs = map[string]*perIter{}
	}
	if g.refs[name] != nil {
		return
	}

	m := &perIter{
		name:  name,
		units: map[string]uint64{},
	}
	g.perIter = append(g.perIter, m)
	g.refs[name] = m
}

func (g *Grabber) add(name string, v any) {
	g.mu.Lock()
	defer g.mu.Unlock()
	m := g.refs[name]
	m.count++
	m.units[fmt.Sprintf("%T", v)]++

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

func (g *Grabber) done(name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	m, ok := g.refs[name]
	if !ok {
		return
	}
	m.done = true
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

func New() *Grabber {
	return &Grabber{
		mark: time.Now(),
		quit: make(chan struct{}),
	}
}

func (g *Grabber) Start() {
	go g.spin()
}

func (g *Grabber) Done() {
	g.mu.Lock()
	defer g.mu.Unlock()
	close(g.quit)
	log.Printf("--- DONE Metrics ---")
	for _, m := range g.perIter {
		log.Printf("  [%s] Total processed: %d closed: %v", m.name, m.count, m.done)
		for k, v := range m.units {
			log.Printf("  [%s]   %s: %d", m.name, k, v)
		}
	}
}

func (g *Grabber) spin() {
	mark := time.Now()
	for {
		select {
		case <-g.quit:
			return
		default:
		}
		time.Sleep(5 * time.Second)
		//if time.Since(mark) < 5*time.Second {
		//	continue
		//}
		running := 0
		func() {
			g.mu.Lock()
			defer g.mu.Unlock()

			secs := time.Since(mark).Seconds()
			log.Printf("--- Metrics %.2fs ---", secs)

			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			log.Printf("  Mem: %s", humanize.Bytes(mem.Alloc))

			for _, m := range g.perIter {
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

func (g *Grabber) Count(it Iter, name string) Iter {
	g.create(name)
	return etl.MakeIter(etl.Custom[any]{
		Next: func(ctx context.Context) (any, error) {
			v, err := it.Next(ctx)
			g.add(name, v)
			return v, err
		},
		Close: func() error {
			g.done(name)
			return it.Close()
		},
	})
}
