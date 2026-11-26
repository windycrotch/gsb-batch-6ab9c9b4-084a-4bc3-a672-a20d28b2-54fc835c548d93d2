// Command pgready reports whether the PostgreSQL DSN in argv[1] can be
// pinged. It exits 0 when ready, 1 otherwise. Used by the acceptance test
// runner to wait on dockerized Postgres without requiring psql locally.
package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: pgready <postgres-dsn>")
		os.Exit(2)
	}
	db, err := sql.Open("postgres", os.Args[1])
	if err != nil {
		os.Exit(1)
	}
	defer db.Close()
	db.SetConnMaxLifetime(5 * time.Second)
	if err := db.Ping(); err != nil {
		os.Exit(1)
	}
}
