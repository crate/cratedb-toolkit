/*
Example program using the official Rockset Golang Client with a CrateDB backend.
Here: "Documents" and "Queries" APIs, basic usage.

Usage
=====
- Adjust configuration: http://localhost:4243
- Run program: go run basic.go

Documentation
=============
- https://pkg.go.dev/github.com/rockset/rockset-go-client#RockClient.AddDocuments
- https://pkg.go.dev/github.com/rockset/rockset-go-client#RockClient.Query
- https://docs.rockset.com/documentation/reference/adddocuments
- https://docs.rockset.com/documentation/reference/query
*/

package main

import (
	"context"
	"fmt"
	"os"
    "time"

	"github.com/rs/zerolog"

    "github.com/rockset/rockset-go-client"
)


func main() {

    console := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
    log := zerolog.New(console).Level(zerolog.TraceLevel).With().Timestamp().Logger()
    ctx := log.WithContext(context.Background())

    // The Golang client does not allow setting the scheme from the URL as it only supports HTTPS.
    // https://github.com/rockset/rockset-go-client/blob/v0.24.2/rockset.go#L114
    // => Error: http: server gave HTTP response to HTTPS client
    rc, err := rockset.NewClient(rockset.WithAPIKey("abc123"), rockset.WithAPIServer("http://localhost:4243"))

    // rc.cfg undefined (type *rockset.RockClient has no field or method cfg)
    //rc.cfg.Scheme = "http://"

    fmt.Println("rc:", rc)
    fmt.Println("err:", err)

    /*
    response, err := rs.AddDocumentsWithOffset(ctx, "commons", "users", docs)
    if err != nil {
        return err
    }
    w := wait.New(rs)
    err = w.UntilQueryable(ctx, "commons", "users", []string{response.GetLastOffset()})
    */

    result, err := rc.Query(ctx, "SELECT * FROM commons.foobar")
    fmt.Println(result)
    fmt.Println(err)


    /*
    q := rc.QueriesApi.Query(ctx)
    rq := openapi.NewQueryRequestWithDefaults()

    rq.Sql = openapi.QueryRequestSql{Query: "SELECT * FROM commons._events where label = :label"}
    rq.Sql.DefaultRowLimit = openapi.PtrInt32(10)

    rq.Sql.Parameters = []openapi.QueryParameter{
        {
            Name:  "label",
            Type:  "string",
            Value: "QUERY_SUCCESS",
        },
    }

    r, _, err := q.Body(*rq).Execute()
    if err != nil {
        log.Fatal(err)
    }

    for _, c := range r.Collections {
        fmt.Printf("collection: %s\n", c)
    }
    */

}
