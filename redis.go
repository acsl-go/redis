package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	goredis "github.com/redis/go-redis/v9"
)

type Client struct {
	client goredis.UniversalClient
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

	return &Client{
		client: client,
		config: cfg,
	}, nil
}

func (client *Client) Get(ctx context.Context, key string, v interface{}) error {
	key_str := client.config.Prefix + ":" + key
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
	key_str := client.config.Prefix + ":" + key
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
	key_str := client.config.Prefix + ":" + key
	data_str, e := json.Marshal(v)
	if e != nil {
		return false, "", errors.Wrap(e, "RedisSetNXEx:JSONMarshal")
	}

	if e := client.client.SetNX(ctx, key_str, data_str, time.Duration(ttl)*time.Second).Err(); e != nil {
		if e == goredis.Nil {
			return false, "", nil
		}
		return false, "", errors.Wrap(e, "RedisSetNXEx")
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
	key_str := client.config.Prefix + ":" + key
	if e := client.client.SetNX(ctx, key_str, v, time.Duration(ttl)*time.Second).Err(); e != nil {
		if e == goredis.Nil {
			return false, nil
		}
		return false, errors.Wrap(e, "RedisSetNX")
	}
	return true, nil
}

func (client *Client) SetStr(ctx context.Context, key string, v string, ttl int) error {
	key_str := client.config.Prefix + ":" + key
	if e := client.client.Set(ctx, key_str, v, time.Duration(ttl)*time.Second).Err(); e != nil {
		return errors.Wrap(e, "RedisSetStr")
	}
	return nil
}

func (client *Client) GetStr(ctx context.Context, key string) (string, error) {
	key_str := client.config.Prefix + ":" + key
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
	key_str := client.config.Prefix + ":" + key
	if e := client.client.Del(ctx, key_str).Err(); e != nil {
		return errors.Wrap(e, "RedisDel")
	}
	return nil
}

func (client *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	key_str := client.config.Prefix + ":" + key
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
	key_str := client.config.Prefix + ":" + key
	if e := client.client.SAdd(ctx, key_str, members...).Err(); e != nil {
		return errors.Wrap(e, "RedisSAdd")
	}
	return nil
}

func (client *Client) SRem(ctx context.Context, key string, members ...interface{}) error {
	key_str := client.config.Prefix + ":" + key
	if e := client.client.SRem(ctx, key_str, members...).Err(); e != nil {
		return errors.Wrap(e, "RedisSRemove")
	}
	return nil
}

func (client *Client) SIsMember(ctx context.Context, key string, member interface{}) (bool, error) {
	key_str := client.config.Prefix + ":" + key
	has, e := client.client.SIsMember(ctx, key_str, member).Result()
	if e != nil {
		return false, errors.Wrap(e, "RedisSHas")
	}
	return has, nil
}

func (client *Client) SMemebers(ctx context.Context, key string) []string {
	key_str := client.config.Prefix + ":" + key
	return client.client.SMembers(ctx, key_str).Val()
}

func (client *Client) Incr(ctx context.Context, key string) (int64, error) {
	key_str := client.config.Prefix + ":" + key
	val, e := client.client.Incr(ctx, key_str).Result()
	if e != nil {
		return 0, errors.Wrap(e, "RedisIncr")
	}
	return val, nil
}

func (client *Client) IncrEx(ctx context.Context, key string, ttl int) (int64, error) {
	key_str := client.config.Prefix + ":" + key
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
	key_str := client.config.Prefix + ":" + key
	val, e := client.client.Decr(ctx, key_str).Result()
	if e != nil {
		return 0, errors.Wrap(e, "RedisDecr")
	}
	return val, nil
}

func (client *Client) DecrEx(ctx context.Context, key string, ttl int) (int64, error) {
	key_str := client.config.Prefix + ":" + key
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
