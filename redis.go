package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/pkg/errors"
	goredis "github.com/redis/go-redis/v9"
)

type Client struct {
	client goredis.UniversalClient
	prefix string
	config *Config
}

var (
	ErrNotFound = errors.New("redis: key not found")
)

func NewClient(cfg *Config) (*Client, error) {
	client := goredis.NewUniversalClient(&goredis.UniversalOptions{
		Addrs:    cfg.Addresses,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		return nil, errors.Wrap(err, "redis: failed to ping")
	}

	pfx := cfg.Prefix
	if pfx != "" {
		pfx += ":"
	}

	return &Client{
		client: client,
		prefix: pfx,
		config: cfg,
	}, nil
}

func (client *Client) Client() goredis.Cmdable {
	return client.client
}

func (client *Client) Get(ctx context.Context, key string, v interface{}) error {
	key_str := client.prefix + key
	data_str, e := client.client.Get(ctx, key_str).Result()
	if e != nil {
		if e == goredis.Nil {
			return ErrNotFound
		}
		return errors.Wrap(e, "RedisGet")
	}

	if data_str == "" {
		return ErrNotFound
	}

	if e := json.Unmarshal([]byte(data_str), v); e != nil {
		return errors.Wrap(e, "RedisGet:JSONUnmarshal")
	}

	return nil
}

func (client *Client) Expire(ctx context.Context, key string, ttl int) error {
	if e := client.client.Expire(ctx, key, time.Duration(ttl)*time.Second).Err(); e != nil {
		return errors.Wrap(e, "RedisExpire")
	}
	return nil
}

func (client *Client) SetEx(ctx context.Context, key string, v interface{}, ttl int) (string, error) {
	key_str := client.prefix + key
	data_str, e := json.Marshal(v)
	if e != nil {
		return "", errors.Wrap(e, "RedisSetEx:JSONMarshal")
	}

	if e := client.client.Set(ctx, key_str, data_str, time.Duration(ttl)*time.Second).Err(); e != nil {
		return "", errors.Wrap(e, "RedisSetEx")
	}

	return string(data_str), nil
}

func (client *Client) SetNXEx(ctx context.Context, key string, v interface{}, ttl int) (bool, string, error) {
	key_str := client.prefix + key
	data_str, e := json.Marshal(v)
	if e != nil {
		return false, "", errors.Wrap(e, "RedisSetNXEx:JSONMarshal")
	}

	if b, e := client.client.SetNX(ctx, key_str, data_str, time.Duration(ttl)*time.Second).Result(); e != nil {
		if e == goredis.Nil {
			return false, "", nil
		}
		return false, "", errors.Wrap(e, "RedisSetNXEx")
	} else if !b {
		return false, "", nil
	}

	return true, string(data_str), nil
}

func (client *Client) Set(ctx context.Context, key string, v interface{}, ttl int) error {
	_, e := client.SetEx(ctx, key, v, ttl)
	return e
}

func (client *Client) SetNX(ctx context.Context, key string, v interface{}, ttl int) (bool, error) {
	b, _, e := client.SetNXEx(ctx, key, v, ttl)
	return b, e
}

func (client *Client) SetNXStr(ctx context.Context, key string, v string, ttl int) (bool, error) {
	key_str := client.prefix + key
	if b, e := client.client.SetNX(ctx, key_str, v, time.Duration(ttl)*time.Second).Result(); e != nil {
		if e == goredis.Nil {
			return false, nil
		}
		return false, errors.Wrap(e, "RedisSetNX")
	} else if !b {
		return false, nil
	}
	return true, nil
}

func (client *Client) SetStr(ctx context.Context, key string, v string, ttl int) error {
	key_str := client.prefix + key
	if e := client.client.Set(ctx, key_str, v, time.Duration(ttl)*time.Second).Err(); e != nil {
		return errors.Wrap(e, "RedisSetStr")
	}
	return nil
}

func (client *Client) GetStr(ctx context.Context, key string) (string, error) {
	key_str := client.prefix + key
	data_str, e := client.client.Get(ctx, key_str).Result()
	if e != nil {
		if e == goredis.Nil {
			return "", ErrNotFound
		}
		return "", errors.Wrap(e, "RedisGetStr")
	}
	return data_str, nil
}

func (client *Client) Del(ctx context.Context, key string) error {
	key_str := client.prefix + key
	if e := client.client.Del(ctx, key_str).Err(); e != nil {
		return errors.Wrap(e, "RedisDel")
	}
	return nil
}

func (client *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	key_str := client.prefix + key
	ttl, e := client.client.TTL(ctx, key_str).Result()
	if e != nil {
		if e == goredis.Nil {
			return 0, ErrNotFound
		}
		return 0, errors.Wrap(e, "RedisTTL")
	}
	return ttl, nil
}

func (client *Client) SAdd(ctx context.Context, key string, members ...interface{}) error {
	key_str := client.prefix + key
	if e := client.client.SAdd(ctx, key_str, members...).Err(); e != nil {
		return errors.Wrap(e, "RedisSAdd")
	}
	return nil
}

func (client *Client) SCard(ctx context.Context, key string) int64 {
	key_str := client.prefix + key
	return client.client.SCard(ctx, key_str).Val()
}

func (client *Client) SRem(ctx context.Context, key string, members ...interface{}) error {
	key_str := client.prefix + key
	if e := client.client.SRem(ctx, key_str, members...).Err(); e != nil {
		return errors.Wrap(e, "RedisSRemove")
	}
	return nil
}

func (client *Client) SIsMember(ctx context.Context, key string, member interface{}) (bool, error) {
	key_str := client.prefix + key
	has, e := client.client.SIsMember(ctx, key_str, member).Result()
	if e != nil {
		return false, errors.Wrap(e, "RedisSHas")
	}
	return has, nil
}

func (client *Client) SMembers(ctx context.Context, key string) []string {
	key_str := client.prefix + key
	return client.client.SMembers(ctx, key_str).Val()
}

func (client *Client) Incr(ctx context.Context, key string) (int64, error) {
	key_str := client.prefix + key
	val, e := client.client.Incr(ctx, key_str).Result()
	if e != nil {
		return 0, errors.Wrap(e, "RedisIncr")
	}
	return val, nil
}

func (client *Client) IncrEx(ctx context.Context, key string, ttl int) (int64, error) {
	key_str := client.prefix + key
	val, e := client.client.Incr(ctx, key_str).Result()
	if e != nil {
		return 0, errors.Wrap(e, "RedisIncr")
	}
	if ttl > 0 {
		if e := client.Expire(ctx, key, ttl); e != nil {
			fmt.Printf("RedisIncr:Expire: %v\n", e) // Only Output Error
		}
	}
	return val, nil
}

func (client *Client) Decr(ctx context.Context, key string) (int64, error) {
	key_str := client.prefix + key
	val, e := client.client.Decr(ctx, key_str).Result()
	if e != nil {
		return 0, errors.Wrap(e, "RedisDecr")
	}
	return val, nil
}

func (client *Client) DecrEx(ctx context.Context, key string, ttl int) (int64, error) {
	key_str := client.prefix + key
	val, e := client.client.Decr(ctx, key_str).Result()
	if e != nil {
		return 0, errors.Wrap(e, "RedisDecr")
	}
	if ttl > 0 {
		if e := client.Expire(ctx, key, ttl); e != nil {
			fmt.Printf("RedisDecr:Expire: %v\n", e) // Only Output Error
		}
	}
	return val, nil
}

func (client *Client) BFReserve(ctx context.Context, key string, size int64, errorRate float64) error {
	key_str := client.prefix + key
	if e := client.client.BFReserve(ctx, key_str, errorRate, size).Err(); e != nil {
		return errors.Wrap(e, "RedisBFReserve")
	}
	return nil
}

func (client *Client) BFAdd(ctx context.Context, key string, members interface{}) error {
	key_str := client.prefix + key
	if e := client.client.BFAdd(ctx, key_str, members).Err(); e != nil {
		return errors.Wrap(e, "RedisBFAdd")
	}
	return nil
}

func (client *Client) BFMAdd(ctx context.Context, key string, members ...interface{}) error {
	key_str := client.prefix + key
	if e := client.client.BFMAdd(ctx, key_str, members...).Err(); e != nil {
		return errors.Wrap(e, "RedisBFMAdd")
	}
	return nil
}

func (client *Client) BFExists(ctx context.Context, key string, members interface{}) (bool, error) {
	key_str := client.prefix + key
	exists, e := client.client.BFExists(ctx, key_str, members).Result()
	if e != nil {
		return false, errors.Wrap(e, "RedisBFExists")
	}
	return exists, nil
}

func (client *Client) BFMExists(ctx context.Context, key string, members ...interface{}) ([]bool, error) {
	key_str := client.prefix + key
	exists, e := client.client.BFMExists(ctx, key_str, members...).Result()
	if e != nil {
		return nil, errors.Wrap(e, "RedisBFMExists")
	}
	return exists, nil
}

func (client *Client) HIncrBy(ctx context.Context, key string, field string, value int64) (int64, error) {
	key_str := client.prefix + key
	val, e := client.client.HIncrBy(ctx, key_str, field, value).Result()
	if e != nil {
		return 0, errors.Wrap(e, "RedisHIncrBy")
	}
	return val, nil
}

func (client *Client) HGet(ctx context.Context, key string, field string) (string, error) {
	key_str := client.prefix + key
	val, e := client.client.HGet(ctx, key_str, field).Result()
	if e != nil {
		if e == goredis.Nil {
			return "", ErrNotFound
		}
		return "", errors.Wrap(e, "RedisHGet")
	}
	return val, nil
}

func (client *Client) HGetI(ctx context.Context, key string, field string) (int64, error) {
	str, err := client.HGet(ctx, key, field)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(str, 10, 64)
}

func (client *Client) HSet(ctx context.Context, key string, field string, value interface{}) error {
	key_str := client.prefix + key
	if e := client.client.HSet(ctx, key_str, field, value).Err(); e != nil {
		return errors.Wrap(e, "RedisHSet")
	}
	return nil
}

func (client *Client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	key_str := client.prefix + key
	val, e := client.client.HGetAll(ctx, key_str).Result()
	if e != nil {
		if e == goredis.Nil {
			return nil, ErrNotFound
		}
		return nil, errors.Wrap(e, "RedisHGetAll")
	}
	return val, nil
}

func (client *Client) HDel(ctx context.Context, key string, field string) error {
	key_str := client.prefix + key
	if e := client.client.HDel(ctx, key_str, field).Err(); e != nil {
		return errors.Wrap(e, "RedisHDel")
	}
	return nil
}

func (client *Client) HKeys(ctx context.Context, key string) ([]string, error) {
	key_str := client.prefix + key
	val, e := client.client.HKeys(ctx, key_str).Result()
	if e != nil {
		if e == goredis.Nil {
			return nil, ErrNotFound
		}
		return nil, errors.Wrap(e, "RedisHKeys")
	}
	return val, nil
}

func (client *Client) HExists(ctx context.Context, key string, field string) (bool, error) {
	key_str := client.prefix + key
	exists, e := client.client.HExists(ctx, key_str, field).Result()
	if e != nil {
		return false, errors.Wrap(e, "RedisHExists")
	}
	return exists, nil
}

func (client *Client) HLen(ctx context.Context, key string) (int64, error) {
	key_str := client.prefix + key
	val, e := client.client.HLen(ctx, key_str).Result()
	if e != nil {
		if e == goredis.Nil {
			return 0, ErrNotFound
		}
		return 0, errors.Wrap(e, "RedisHLen")
	}
	return val, nil
}

func (client *Client) HSetNX(ctx context.Context, key string, field string, value interface{}) (bool, error) {
	key_str := client.prefix + key
	val, e := client.client.HSetNX(ctx, key_str, field, value).Result()
	if e != nil {
		return false, errors.Wrap(e, "RedisHSetNX")
	}
	return val, nil
}

func (client *Client) HSetNXEx(ctx context.Context, key string, field string, value interface{}, ttl int) (bool, error) {
	key_str := client.prefix + key
	val, e := client.client.HSetNX(ctx, key_str, field, value).Result()
	if e != nil {
		return false, errors.Wrap(e, "RedisHSetNX")
	}
	if val && ttl > 0 {
		if e := client.Expire(ctx, key, ttl); e != nil {
			fmt.Printf("RedisHSetNX:Expire: %v\n", e) // Only Output Error
		}
	}
	return val, nil
}

func (client *Client) HSetEx(ctx context.Context, key string, field string, value interface{}, ttl int) error {
	key_str := client.prefix + key
	if e := client.client.HSet(ctx, key_str, field, value).Err(); e != nil {
		return errors.Wrap(e, "RedisHSetEx")
	}
	if ttl > 0 {
		if e := client.Expire(ctx, key, ttl); e != nil {
			fmt.Printf("RedisHSetEx:Expire: %v\n", e) // Only Output Error
		}
	}
	return nil
}

func (client *Client) ZAddMember(ctx context.Context, key string, member interface{}, score float64) error {
	key_str := client.prefix + key
	if e := client.client.ZAdd(ctx, key_str, goredis.Z{
		Member: member,
		Score:  score,
	}).Err(); e != nil {
		return errors.Wrap(e, "RedisZAddMember")
	}
	return nil
}

func (client *Client) ZRangeWithScores(ctx context.Context, key string, start int64, stop int64) ([]goredis.Z, error) {
	key_str := client.prefix + key
	val, e := client.client.ZRangeWithScores(ctx, key_str, start, stop).Result()
	if e != nil {
		return nil, errors.Wrap(e, "RedisZRangeWithScore")
	}
	return val, nil
}
