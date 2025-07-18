package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type FieldMetaData struct {
	Field      string
	Type       string
	Collation  bool
	Null       string
	Key        string
	Default    string
	Extra      string
	Privileges string
	Comment    string
}

type DbExplorer struct {
	DB         *sql.DB
	TableNames []string
	Data       map[string][]FieldMetaData
}

func (d *DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	method := r.Method
	if path == "/" && method == http.MethodGet {
		d.writeTables(w)
		return
	}

	cutFirstSlash := path[1:]
	tableName := cutFirstSlash[:strings.Index(cutFirstSlash, "/")]
	if !slices.Contains(d.TableNames, tableName) {
		http.NotFound(w, r)
	}
}

func NewDbExplorer(db *sql.DB) (*DbExplorer, error) {
	tablesRs, err := db.Query("SHOW TABLES;")
	if err != nil {
		return nil, err
	}

	tablesData := make(map[string][]FieldMetaData)
	for tablesRs.Next() {
		var name string
		if err := tablesRs.Scan(&name); err != nil {
			return nil, err
		}

		tablesData[name] = nil
	}
	tablesRs.Close()

	for tableName, _ := range tablesData {
		fieldsRs, err := db.Query(fmt.Sprintf("SHOW FULL COLUMNS FROM `%s`;", tableName))
		if err != nil {
			return nil, err
		}
		fieldMetaData := make([]FieldMetaData, 0)
		for fieldsRs.Next() {
			field := FieldMetaData{}
			fieldsRs.Scan(
				&field.Field, &field.Type, &field.Collation,
				&field.Null, &field.Key, &field.Default,
				&field.Extra, &field.Privileges, &field.Comment,
			)
			fieldMetaData = append(fieldMetaData, field)
		}
		tablesData[tableName] = fieldMetaData
		fieldsRs.Close()
	}

	return &DbExplorer{DB: db, Data: tablesData, TableNames: extractTableNames(tablesData)}, nil
}

func (d *DbExplorer) writeTables(w http.ResponseWriter) {
	bytes, err := json.Marshal(struct {
		Response interface{} `json:"response"`
	}{struct {
		Tables []string `json:"tables"`
	}{d.TableNames}})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Write(bytes)
}

func extractTableNames(data map[string][]FieldMetaData) []string {
	tables := make([]string, 0, len(data))
	i := 0
	for k, _ := range data {
		tables = append(tables, k)
		i++
	}
	return tables
}
