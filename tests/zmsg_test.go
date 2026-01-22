package zmsg_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yourorg/zmsg"
)

func TestZMsg_BasicOperations(t *testing.T) {
	ctx := context.Background()

	cfg := zmsg.DefaultConfig()
	cfg.PostgresDSN = "postgresql://test:test@localhost/test?sslmode=disable"
	cfg.RedisAddr = "localhost:6379"

	zm, err := zmsg.New(ctx, cfg)
	require.NoError(t, err)
	defer zm.Close()

	// Test CacheOnly
	key := "test_key_1"
	value := []byte("test_value_1")

	err = zm.CacheOnly(ctx, key, value, zmsg.WithTTL(time.Hour))
	assert.NoError(t, err)

	// Test Get
	retrieved, err := zm.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrieved)

	// Test Del
	err = zm.Del(ctx, key)
	assert.NoError(t, err)

	// Test Get after delete
	_, err = zm.Get(ctx, key)
	assert.Error(t, err)
}

func TestZMsg_CacheAndStore(t *testing.T) {
	ctx := context.Background()

	cfg := zmsg.DefaultConfig()
	cfg.PostgresDSN = "postgresql://test:test@localhost/test?sslmode=disable"
	cfg.RedisAddr = "localhost:6379"

	zm, err := zmsg.New(ctx, cfg)
	require.NoError(t, err)
	defer zm.Close()

	// Test CacheAndStore
	key := "test_feed_1"
	value := []byte(`{"content": "Hello World"}`)

	sqlTask := &zmsg.SQLTask{
		Query:  "INSERT INTO test_feeds (id, content) VALUES ($1, $2)",
		Params: []interface{}{key, value},
	}

	returnedID, err := zm.CacheAndStore(ctx, key, value, sqlTask)
	assert.NoError(t, err)
	assert.NotEmpty(t, returnedID)

	// Verify cache
	cached, err := zm.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value, cached)
}

func TestZMsg_Counter(t *testing.T) {
	ctx := context.Background()

	cfg := zmsg.DefaultConfig()
	cfg.PostgresDSN = "postgresql://test:test@localhost/test?sslmode=disable"
	cfg.RedisAddr = "localhost:6379"

	zm, err := zmsg.New(ctx, cfg)
	require.NoError(t, err)
	defer zm.Close()

	// Test batch counter
	feedID := "feed_123"

	// Simulate 1000 likes
	for i := 0; i < 1000; i++ {
		userID := fmt.Sprintf("user_%d", i)
		key := fmt.Sprintf("like:%s:%s", feedID, userID)

		sqlTask := &zmsg.SQLTask{
			Query:    "INSERT INTO feed_likes (feed_id, user_id) VALUES ($1, $2)",
			Params:   []interface{}{feedID, userID},
			TaskType: zmsg.TaskTypeCount,
			BatchKey: fmt.Sprintf("like_count:%s", feedID),
		}

		err = zm.CacheAndDelayStore(ctx, key, []byte("1"), sqlTask)
		assert.NoError(t, err)
	}

	// Wait for batch flush
	time.Sleep(6 * time.Second)
}

func TestZMsg_NextID(t *testing.T) {
	ctx := context.Background()

	cfg := zmsg.DefaultConfig()
	cfg.PostgresDSN = "postgresql://test:test@localhost/test?sslmode=disable"
	cfg.RedisAddr = "localhost:6379"

	zm, err := zmsg.New(ctx, cfg)
	require.NoError(t, err)
	defer zm.Close()

	// Test ID generation
	id1, err := zm.NextID(ctx, "feed")
	assert.NoError(t, err)
	assert.Contains(t, id1, "feed_")

	id2, err := zm.NextID(ctx, "user")
	assert.NoError(t, err)
	assert.Contains(t, id2, "user_")

	assert.NotEqual(t, id1, id2)
}

func TestZMsg_BatchOperation(t *testing.T) {
	ctx := context.Background()

	cfg := zmsg.DefaultConfig()
	cfg.PostgresDSN = "postgresql://test:test@localhost/test?sslmode=disable"
	cfg.RedisAddr = "localhost:6379"

	zm, err := zmsg.New(ctx, cfg)
	require.NoError(t, err)
	defer zm.Close()

	// Create batch
	batch := zm.Batch()

	// Add multiple operations
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("batch_key_%d", i)
		value := []byte(fmt.Sprintf("batch_value_%d", i))

		batch.CacheOnly(key, value, zmsg.WithTTL(time.Hour))
	}

	assert.Equal(t, 10, batch.Size())

	// Execute batch
	err = batch.Execute(ctx)
	assert.NoError(t, err)

	// Verify all items are cached
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("batch_key_%d", i)
		value, err := zm.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("batch_value_%d", i)), value)
	}
}

func TestZMsg_QueryPipeline(t *testing.T) {
	ctx := context.Background()

	cfg := zmsg.DefaultConfig()
	cfg.PostgresDSN = "postgresql://test:test@localhost/test?sslmode=disable"
	cfg.RedisAddr = "localhost:6379"

	zm, err := zmsg.New(ctx, cfg)
	require.NoError(t, err)
	defer zm.Close()

	key := "pipeline_test"
	value := []byte("pipeline_value")

	// First query should go to DB
	_, err = zm.Get(ctx, key)
	assert.Error(t, err) // Not found

	// Store it
	sqlTask := &zmsg.SQLTask{
		Query:  "INSERT INTO test_data (id, data) VALUES ($1, $2)",
		Params: []interface{}{key, value},
	}

	_, err = zm.CacheAndStore(ctx, key, value, sqlTask)
	assert.NoError(t, err)

	// Second query should hit cache
	retrieved, err := zm.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrieved)
}
