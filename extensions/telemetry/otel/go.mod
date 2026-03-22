module github.com/sopranoworks/gekka-extensions-telemetry-otel

go 1.26.1

require (
	github.com/sopranoworks/gekka v0.13.0-dev
	github.com/sopranoworks/gekka-config v1.0.4
	go.opentelemetry.io/otel v1.42.0
	go.opentelemetry.io/otel/metric v1.42.0
	go.opentelemetry.io/otel/trace v1.42.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
)

replace github.com/sopranoworks/gekka => ../../../
