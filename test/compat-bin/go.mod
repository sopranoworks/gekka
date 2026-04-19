module github.com/sopranoworks/gekka/test/compat-bin

go 1.26.1

require github.com/sopranoworks/gekka v1.0.0-rc1

require (
	github.com/sopranoworks/gekka-config v1.0.4 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/sopranoworks/gekka => ../../
