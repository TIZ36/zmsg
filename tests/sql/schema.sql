CREATE TABLE IF NOT EXISTS feeds (
    id TEXT PRIMARY KEY,
    content TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS feed_meta (
    id TEXT PRIMARY KEY,
    like_count BIGINT DEFAULT 0,
    reply_count BIGINT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    name TEXT,
    balance BIGINT DEFAULT 0,
    meta JSONB DEFAULT '{}',
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS replies (
    id TEXT PRIMARY KEY,
    feed_id TEXT,
    user_id TEXT,
    content TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS likes (
    user_id TEXT NOT NULL,
    feed_id TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, feed_id)
);

CREATE TABLE IF NOT EXISTS zmsg_data (
    id TEXT PRIMARY KEY,
    data BYTEA,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
