module github.com/sopranoworks/gekka-extensions-persistence-cassandra

go 1.26.1

require (
	github.com/gocql/gocql v1.7.0
	github.com/google/uuid v1.6.0
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/sopranoworks/gekka v0.14.0-dev
	github.com/sopranoworks/gekka-config v1.0.4
)

require (
	github.com/golang/snappy v0.0.3 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/sopranoworks/gekka => ../../../
