package pika_integration

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// 测试场景：执行del命令，在执行bgsave之后，get数据会出错
func TestBgsave(t *testing.T) {
	var err error

	err = rdb.Set(ctx, "bgsava_key", "bgsava_value", 0).Err()
	assert.Nil(t, err)
	err = rdb.Set(ctx, "bgsava_key2", "bgsava_value3", 0).Err()
	assert.Nil(t, err)
	err = rdb.HSet(ctx, "bgsava_key3", "bgsava_value", 0).Err()
	assert.Nil(t, err)

	res, err := rdb.BgSave(ctx).Result()
	assert.Nil(t, err)
	assert.Equal(t, "Background saving started", res)

}

func TestBgsaveAndDel(t *testing.T) {
	var err error

	err = rdb.Set(ctx, "bgsava_key", "bgsava_value", 0).Err()
	assert.Nil(t, err)
	err = rdb.Set(ctx, "bgsava_key2", "bgsava_value2", 0).Err()
	assert.Nil(t, err)
	err = rdb.Set(ctx, "bgsava_key3", "bgsava_value3", 0).Err()
	assert.Nil(t, err)
	err = rdb.HSet(ctx, "bgsava_key4", "bgsava_value4", 0).Err()
	assert.Nil(t, err)

	err = rdb.Del(ctx, "bgsava_key").Err()
	assert.Nil(t, err)

	res, err := rdb.BgSave(ctx).Result()
	assert.Nil(t, err)
	assert.Equal(t, "Background saving started", res)

	val, err := rdb.Get(ctx, "bgsava_key2").Result()
	assert.Nil(t, err)
	assert.Equal(t, val, "bgsava_value2")

	err = rdb.Del(ctx, "bgsava_key4").Err()
	assert.Nil(t, err)

	val, err = rdb.Get(ctx, "bgsava_key3").Result()
	assert.Nil(t, err)
	assert.Equal(t, val, "bgsava_value3")
}
