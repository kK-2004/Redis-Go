package handler

type Option func(*Conf)

type Conf struct {
	Strategy string
	Use_db   string
}

func WithStrategy(strategy string) Option {
	return func(c *Conf) {
		c.Strategy = strategy
	}
}

func WithDB(db string) Option {
	return func(c *Conf) {
		c.Use_db = db
	}
}
