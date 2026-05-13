module github.com/kamalyes/go-casbin-redis-adapter

go 1.25.0

require (
	github.com/kamalyes/go-cachex v0.1.9
	github.com/kamalyes/go-casbin v0.0.0-20260513071529-b67c4efbc554
	github.com/kamalyes/go-logger v0.4.6
	github.com/kamalyes/go-toolbox v0.12.0
	github.com/redis/go-redis/v9 v9.18.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgraph-io/ristretto/v2 v2.4.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/kamalyes/go-jsonpath v0.0.0-20260129163507-0b67ed48bb28 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/crypto v0.50.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	google.golang.org/grpc v1.80.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// 本地开发替换
// replace github.com/kamalyes/go-toolbox => ../go-toolbox

// replace github.com/kamalyes/go-logger => ../go-logger

// replace github.com/kamalyes/go-casbin => ../go-casbin
