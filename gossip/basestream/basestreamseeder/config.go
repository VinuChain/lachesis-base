package basestreamseeder

type Config struct {
	SenderThreads           int
	MaxSenderTasks          int
	MaxPendingResponsesSize int64
	MaxResponsePayloadNum   uint32
	MaxResponsePayloadSize  uint64
	MaxResponseChunks       uint32
	MaxGlobalSessions       int
}

func (c *Config) maxGlobalSessions() int {
	if c.MaxGlobalSessions <= 0 {
		return 1000
	}
	return c.MaxGlobalSessions
}
