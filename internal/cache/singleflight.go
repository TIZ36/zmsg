package cache

import (
	"context"
	"sync"
	"time"
)

// SingleFlightGroup SingleFlight 包装器
type SingleFlightGroup struct {
	group singleflight.Group
	mu    sync.RWMutex
	cache map[string]time.Time
}

// NewSingleFlightGroup 创建 SingleFlight 组
func NewSingleFlightGroup() *SingleFlightGroup {
	return &SingleFlightGroup{
		cache: make(map[string]time.Time),
	}
}

// Do 执行函数
func (g *SingleFlightGroup) Do(ctx context.Context, key string,
	fn func() ([]byte, error)) ([]byte, error) {

	g.mu.RLock()
	if _, loading := g.cache[key]; loading {
		g.mu.RUnlock()

		// 等待其他 goroutine 完成
		for i := 0; i < 10; i++ {
			time.Sleep(time.Millisecond * 10)
			g.mu.RLock()
			_, loading := g.cache[key]
			g.mu.RUnlock()
			if !loading {
				break
			}
		}
	} else {
		g.mu.RUnlock()
	}

	val, err, _ := g.group.Do(key, func() (interface{}, error) {
		g.mu.Lock()
		g.cache[key] = time.Now()
		g.mu.Unlock()

		defer func() {
			g.mu.Lock()
			delete(g.cache, key)
			g.mu.Unlock()
		}()

		return fn()
	})

	if err != nil {
		return nil, err
	}

	return val.([]byte), nil
}

// Forget 忘记 key
func (g *SingleFlightGroup) Forget(key string) {
	g.group.Forget(key)
}
