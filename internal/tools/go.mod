module sigs.k8s.io/kustomize/internal/tools

// separate go.mod file to make explicit that the kustomize command does
// not use or run any code from any of the dependencies in this subdirectory.
// This is true by virtue of two facts: this module imports kustomize and go
// does not allow circular dependencies, and it would show up under the main
// kustomize go.mod file.

go 1.12

require (
	github.com/elastic/go-elasticsearch/v6 v6.8.2
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/gorilla/mux v1.7.3
	github.com/gregjones/httpcache v0.0.0-20190611155906-901d90724c79
	github.com/rs/cors v1.6.0
	k8s.io/api v0.0.0-20190809220925-3ab596449d6f // indirect
	sigs.k8s.io/kustomize/v3 v3.1.1-0.20190822235944-40c613d0cda8
	sigs.k8s.io/yaml v1.1.0
)
