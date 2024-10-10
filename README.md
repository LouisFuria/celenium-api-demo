# celenium-api-demo

Code with demonstration on how to use Celenium API

## Prerequisites

- Go
- PostgreSQL 15+
- Running Rollup to test changes in its performance. Can be used [dummy_rollup](https://github.com/jcstein/dummy_rollup) by @jcstein
- Grafana [optional]

### For running Grafana on Mac:

```sh
brew update
brew install grafana
brew services start grafana
// brew services list // check status if services
```

## Usage

Install dependencies

```sh
go get github.com/lib/pq
```

### Run

```
go run main.go
```
