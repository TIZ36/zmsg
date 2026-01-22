-- 添加状态字段
ALTER TABLE feeds ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'active';

-- 添加状态索引
CREATE INDEX IF NOT EXISTS idx_feeds_status ON feeds(status);
