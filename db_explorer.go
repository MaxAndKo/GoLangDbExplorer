package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
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
	DB                *sql.DB
	TableNames        []string
	Data              map[string][]FieldMetaData
	LimitOffsetRegexp *regexp.Regexp
	ByIdRegexp        *regexp.Regexp
	GetQuery          string
	LimitOffsetQuery  string
	GetByIdQuery      string
}

func (d *DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	method := r.Method
	if path == "/" && method == http.MethodGet {
		d.writeTables(w)
		return
	}

	tableName := extractFuncName(path)
	afterTable := path[len(tableName)+1:]
	if !slices.Contains(d.TableNames, tableName) {
		writeError(w, "unknown table", http.StatusNotFound)
		return
	}

	if method == http.MethodGet && afterTable == "" {
		err := getRows(tableName, d, w)
		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
		}
	}

	if method == http.MethodGet && d.LimitOffsetRegexp.MatchString(afterTable) {
		err := getWithLimitAndOffset(tableName, afterTable, d)
		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
		}
	}

	if method == http.MethodGet && d.ByIdRegexp.MatchString(afterTable) {
		//getById()
	}

	if method == http.MethodPut {
		//putRow()
	}

	if method == http.MethodPost && d.ByIdRegexp.MatchString(afterTable) {
		//updateRow()
	}

	if method == http.MethodDelete && d.ByIdRegexp.MatchString(afterTable) {
		//deleteRow()
	}

}

func getRows(table string, d *DbExplorer, w http.ResponseWriter) error {
	tableData := d.Data[table]
	fieldNames := make([]string, len(tableData))
	for i, datum := range tableData {
		fieldNames[i] = datum.Field
	}

	rs, err := d.DB.Query(fmt.Sprintf(d.GetQuery, strings.Join(fieldNames, ", "), table))
	if err != nil {
		return err
	}
	defer rs.Close()

	countOfFields := len(tableData)

	result := make([]map[string]interface{}, 0)
	for rs.Next() {
		convertedRs := make([]interface{}, countOfFields)
		convertedRsPointers := make([]interface{}, countOfFields)
		for i := range convertedRsPointers {
			convertedRsPointers[i] = &convertedRs[i]
		}
		err := rs.Scan(convertedRsPointers...)
		if err != nil {
			return err
		}

		for i := range convertedRs {
			if convertedRs[i] != nil {
				convertedRs[i] = string(convertedRs[i].([]byte))
			} else {
				convertedRs[i] = nil
			}
		}

		resultMap := make(map[string]interface{}, countOfFields)
		for i, row := range convertedRs {
			resultMap[fieldNames[i]] = row
		}
		result = append(result, resultMap)

		fmt.Sprintf("")
	}

	marshal, err := json.Marshal(result)
	if err != nil {
		return err
	}

	w.Write(marshal) //TODO писать в response - выделить общий метод для этого
	return nil
}

func getWithLimitAndOffset(table string, limitAndOffset string, d *DbExplorer) error {
	limit, err := extractLimitOrOffset(limitAndOffset, "limit")
	if err != nil {
		return err
	}
	offset, err := extractLimitOrOffset(limitAndOffset, "offset")
	if err != nil {
		return err
	}

	rs, err := d.DB.Query(fmt.Sprintf(d.LimitOffsetQuery, table), limit, offset)
	if err != nil {
		return err
	}
	defer rs.Close()

	var res interface{}
	for rs.Next() {
		rs.Scan(&res)
		fmt.Sprintf("")
	}

	return nil
}

func extractLimitOrOffset(limitAndOffset string, targetName string) (int, error) {
	var targetValue int
	targetIndex := strings.Index(limitAndOffset, targetName)
	if targetIndex == -1 {
		return -1, nil
	}
	cutLimit := limitAndOffset[len(targetName)+1:]
	ampersandIndex := strings.Index(cutLimit, "&")
	if ampersandIndex == -1 {
		var err error
		targetValue, err = strconv.Atoi(cutLimit)
		if err != nil {
			return -1, err
		}
	} else {
		var err error
		targetValue, err = strconv.Atoi(cutLimit[:ampersandIndex])
		if err != nil {
			return -1, err
		}
	}

	return targetValue, nil
}

func extractFuncName(path string) string {
	cutFirstSlash := path[1:]
	tableNameEnd := strings.Index(cutFirstSlash, "/")
	var tableName string
	if tableNameEnd == -1 {
		tableName = cutFirstSlash
	} else {
		tableName = cutFirstSlash[:tableNameEnd]
	}
	return tableName
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

	limitOffset, err := regexp.Compile("^/\\?limit=\\d+&offset=\\d+$")
	if err != nil {
		return nil, err
	}

	byId, err := regexp.Compile("^/\\d+$")
	if err != nil {
		return nil, err
	}

	return &DbExplorer{
		DB:                db,
		Data:              tablesData,
		TableNames:        extractTableNames(tablesData),
		LimitOffsetRegexp: limitOffset,
		ByIdRegexp:        byId,
		GetQuery:          "SELECT %s FROM `%s`",
		LimitOffsetQuery:  "SELECT * FROM `%s` LIMIT ? OFFSET ?",
		GetByIdQuery:      "SELECT * FROM `%s` WHERE `%s` = ?",
	}, nil
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

func writeError(w http.ResponseWriter, error string, statusCode int) {
	http.Error(w, error, statusCode)
}
