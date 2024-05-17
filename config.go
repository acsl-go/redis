package redis

type Config struct {
	Addresses []string `mapstructure:"addresses"`
	Password  string   `mapstructure:"password"`
	DB        int      `mapstructure:"database"`
	Prefix    string   `mapstructure:"prefix"`
}
