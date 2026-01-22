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

// WithSyncPersist 设置同步持久化
func WithSyncPersist() Option {
	return func(o *Options) {
		o.SyncPersist = true
	}
}

// WithPriority 设置优先级
func WithPriority(p int) Option {
	return func(o *Options) {
		o.Priority = p
	}
}

// WithTags 设置标签
func WithTags(tags ...string) Option {
	return func(o *Options) {
		o.Tags = tags
	}
}

// buildOptions 构建选项
func buildOptions(opts ...Option) Options {
	o := Options{
		TTL:         24 * time.Hour,
		Consistency: ConsistencyEventual,
		SyncPersist: false,
		Priority:    0,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
