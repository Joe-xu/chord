package chord

import (
	"time"
)

const defaultRPCTimeout = 5 * time.Second

var rpcTimeout = defaultRPCTimeout
