# LanceDB Go SDK

A Go library for [LanceDB](https://github.com/lancedb/lancedb).

## Installation

```bash
go get github.com/lancedb/lancedb/golang/pkg
```

## Usage

### Basic Example

```go
import lancedb "github.com/lancedb/lancedb/golang/pkg"

// Connect to a database
ctx := context.Background()
db, err := lancedb.Connect(ctx, "data/sample-lancedb", nil)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Create a table with schema
schema := lancedb.NewSchemaBuilder().
    AddInt32Field("id", false).
    AddStringField("text", false).
    AddVectorField("vector", 128, lancedb.VectorDataTypeFloat32, false).
    Build()

table, err := db.CreateTable(ctx, "my_table", *schema)
if err != nil {
    log.Fatal(err)
}
defer table.Close()

// Perform vector search
queryVector := []float32{0.1, 0.3, /* ... 128 dimensions */}
results, err := table.VectorSearch("vector", queryVector, 20)
if err != nil {
    log.Fatal(err)
}
fmt.Println(results)
```

The [quickstart guide](https://lancedb.github.io/lancedb/guides/tables/) contains more complete examples.

## Development

See [CONTRIBUTING.md](./CONTRIBUTING.md) for information on how to contribute to LanceDB.

Build:

```shell
make build
```