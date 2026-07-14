module github.com/sopranoworks/gekka/test/compat-bin

go 1.26.1

require github.com/sopranoworks/gekka v1.0.0-rc4

require (
	github.com/sopranoworks/gekka-config v1.0.4 // indirect
	go.etcd.io/bbolt v1.4.3 // indirect
	golang.org/x/sys v0.42.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/sopranoworks/gekka => ../../
