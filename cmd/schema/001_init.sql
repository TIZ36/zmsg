-- 迁移版本表（用于增量迁移）
CREATE TABLE IF NOT EXISTS zmsg_migrations (
    version VARCHAR(255) PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT NOW()
);

-- Feed 表
CREATE TABLE IF NOT EXISTS feeds (
    id VARCHAR(255) PRIMARY KEY,
    user_id BIGINT NOT NULL,
    content TEXT,
    like_count BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 点赞表
CREATE TABLE IF NOT EXISTS feed_likes (
    feed_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    liked_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (feed_id, user_id)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_feeds_user ON feeds(user_id);
CREATE INDEX IF NOT EXISTS idx_feeds_created ON feeds(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_feed_likes_feed ON feed_likes(feed_id);
