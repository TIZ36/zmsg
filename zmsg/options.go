package zmsg

import "time"

// WithTTL 设置过期时间
func WithTTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.TTL = ttl
	}
}

// WithConsistency 设置一致性级别
func WithConsistency(c Consistency) Option {
	return func(o *Options) {
		o.Consistency = c
	}
}

// WithAsyncDelay 设置 asynq 任务延迟时间
func WithAsyncDelay(delay time.Duration) Option {
	return func(o *Options) {
		o.AsyncDelay = delay
	}
}

// buildOptions 构建选项
func buildOptions(opts ...Option) Options {
	o := Options{
		TTL:         24 * time.Hour,
		Consistency: ConsistencyEventual,
		AsyncDelay:  0,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
