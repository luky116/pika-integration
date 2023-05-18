package pika_integration

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	ctx = context.Background()
	rdb *redis.Client
)

func init() {
	rdb = redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:9221",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func TestGetSetDel(t *testing.T) {
	var tests = []struct {
		key   string
		value interface{}
	}{
		{"KEY1", "VALUE1"},
		{"KEY2", "VALUE2"},
		{"KEY_3", "1234"},
		{"KEY__4", "1.091212"},
		{"KEY_%%$5", "0"},
		{"__...2121KEY_%%$6", "Pika"},
		{"KEY1", "VALUE1_multi"},
		{"KEY2", "VALUE2_mkl"},
		{"121KEY__4", "1.0976"},
		{"KEY_%%$5", "990"},
		{"1212KEY_NULL", nil},
	}

	for _, test := range tests {
		if test.value == nil {
			_, err := rdb.Get(ctx, test.key).Result()
			assert.Equal(t, err, redis.Nil)

			rdb.Exists(ctx, test.key).Result()
			continue
		}

		err := rdb.Set(ctx, test.key, test.value, 0).Err()
		assert.Nil(t, err)

		val, err := rdb.Get(ctx, test.key).Result()
		assert.Nil(t, err)
		assert.Equal(t, val, test.value)
	}

	// test del
	err := rdb.Del(ctx, tests[4].key).Err()
	assert.Nil(t, err)
	_, err = rdb.Get(ctx, tests[4].key).Result()
	assert.Equal(t, err, redis.Nil)

	err = rdb.Del(ctx, tests[6].key).Err()
	assert.Nil(t, err)
	_, err = rdb.Get(ctx, tests[6].key).Result()
	assert.Equal(t, err, redis.Nil)

	val, err := rdb.Get(ctx, tests[7].key).Result()
	assert.Nil(t, err)
	assert.Equal(t, val, tests[7].value)

	// test expire
	err = rdb.SetEx(ctx, tests[7].key, tests[7].value, 3*time.Second).Err()
	assert.Nil(t, err)
	val, err = rdb.Get(ctx, tests[7].key).Result()
	assert.Nil(t, err)
	assert.Equal(t, val, tests[7].value)
	time.Sleep(5 * time.Second)
	_, err = rdb.Get(ctx, tests[7].key).Result()
	assert.Equal(t, err, redis.Nil)
}

func TestKeys(t *testing.T) {
	var tests = []struct {
		key   string
		value interface{}
	}{
		{"__KEY1", "VALUE1"},
		{"__KEY2", "VALUE2"},
		{"__KEY_3", "1234"},
		{"__KEY__4", "1.091212"},
		{"__KEY_%%$5", "0"},
		{"__KEY_%%$5", "01212"},
		{"__KEY_%%$6", "Pika"},
		{"__KEY_%%$6", "Pika2"},
		{"__KEY_%%$6", "Pika23"},
	}

	for _, test := range tests {
		err := rdb.Set(ctx, test.key, test.value, 0).Err()
		assert.Nil(t, err)
	}

	res, err := rdb.Keys(ctx, "__KEY*").Result()
	assert.Nil(t, err)

	expect := []string{"__KEY1", "__KEY2", "__KEY_3", "__KEY__4", "__KEY_%%$5", "__KEY_%%$6"}
	assert.Equal(t, len(expect), len(res))
	for _, s := range expect {
		exists := false
		for _, re := range res {
			if s == re {
				exists = true
				break
			}
		}
		if !exists {
			assert.Fail(t, "keys * not equal", expect, res)
		}
	}

}
