// Package itermetrics contains utils to debug and measure iterators.
package etlmetrics

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/stdiopt/danda/etl"
	"github.com/stdiopt/danda/util/humanize"
)

// Iter alias of iter.Iter
type Iter = etl.Iter

// DebugCount prints the number of elements fetched from the iterator.
func DebugCount(it Iter, prefix string) Iter {
	count := 0
	oldCount := 0
	mark := time.Now()
	log := log.New(log.Writer(), prefix, log.Flags())
	units := map[string]int{}
	return etl.MakeIter(etl.Custom[any]{
		Next: func(ctx context.Context) (any, error) {
			count++
			if time.Since(mark) > 5*time.Second {
				mark = time.Now()
				diff := count - oldCount
				oldCount = count

				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)

				log.Printf("Processed: %s (%s/s) mem usage: %s",
					humanize.Number(uint64(count)),
					humanize.Number(float64(diff)/5),
					humanize.Bytes(mem.Alloc),
				)
			}
			v, err := it.Next(ctx)
			units[fmt.Sprintf("%T", v)]++
			return v, err
		},
		Close: func() error {
			log.Printf("%s Total processed: %d", prefix, count)
			for k, v := range units {
				log.Printf("%s   %s: %d", prefix, k, v)
			}
			return it.Close()
		},
	})
}
