package main

import (
	"database/sql"
	"fmt"
	"net/http"
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
	DB *sql.DB
}

func (*DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {

}

func NewDbExplorer(db *sql.DB) (*DbExplorer, error) {
	tablesRs, err := db.Query("SHOW TABLES;")
	if err != nil {
		return nil, err
	}

	tableNames := make(map[string][]FieldMetaData)
	for tablesRs.Next() {
		var name string
		if err := tablesRs.Scan(&name); err != nil {
			return nil, err
		}

		tableNames[name] = nil
	}
	tablesRs.Close()

	for tableName, _ := range tableNames {
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
		tableNames[tableName] = fieldMetaData
		fieldsRs.Close()
	}

	return &DbExplorer{DB: db}, nil
}
