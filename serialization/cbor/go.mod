module github.com/sopranoworks/gekka/serialization/cbor

go 1.26.1

replace github.com/sopranoworks/gekka => ../../

require (
	github.com/fxamacker/cbor/v2 v2.9.1
	github.com/sopranoworks/gekka v0.0.0
)

require github.com/x448/float16 v0.8.4 // indirect
