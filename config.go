package redis

type Config struct {
	Addresses []string `mapstructure:"addresses" json:"addresses" yaml:"addresses"`
	Password  string   `mapstructure:"password" json:"password" yaml:"password"`
	DB        int      `mapstructure:"database" json:"database" yaml:"database"`
	Prefix    string   `mapstructure:"prefix" json:"prefix" yaml:"prefix"`
}
