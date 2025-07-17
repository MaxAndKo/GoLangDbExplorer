package main

import (
	"database/sql"
	"fmt"
	"net/http"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type DbExplorer struct {
	DB *sql.DB
}

func (*DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {

}

func NewDbExplorer(db *sql.DB) (*DbExplorer, error) {
	tables, err := db.Query("SHOW TABLES;")
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, 0)
	for tables.Next() {
		var name string
		if err := tables.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}

	fmt.Println(tables)
	tables.Close()
	return &DbExplorer{DB: db}, nil
}
