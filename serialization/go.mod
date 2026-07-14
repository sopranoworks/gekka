module github.com/sopranoworks/gekka/serialization

go 1.26.1

replace github.com/sopranoworks/gekka => ../

require (
	github.com/fxamacker/cbor/v2 v2.9.2
	github.com/sopranoworks/gekka v1.0.0-rc4
)

require github.com/x448/float16 v0.8.4 // indirect
