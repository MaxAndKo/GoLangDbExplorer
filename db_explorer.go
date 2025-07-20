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
	queryParams := r.URL.RawQuery
	method := r.Method
	if path == "/" && method == http.MethodGet {
		writeTables(w, d.TableNames)
		return
	}

	tableName := extractFuncName(path)
	afterTable := path[len(tableName)+1:]
	if !slices.Contains(d.TableNames, tableName) {
		writeError(w, "unknown table", http.StatusNotFound)
		return
	}

	if method == http.MethodGet && afterTable == "" && queryParams == "" {
		err := getRows(tableName, d, w)
		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
		}
	}

	if method == http.MethodGet && d.LimitOffsetRegexp.MatchString(queryParams) {
		err := getWithLimitAndOffset(tableName, afterTable, d, w)
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
	fieldNames := extractFieldNames(tableData)

	rs, err := d.DB.Query(fmt.Sprintf(d.GetQuery, strings.Join(fieldNames, "`, `"), table))
	if err != nil {
		return err
	}
	result, err := executeGetQuery(rs, tableData)
	rs.Close()
	if err != nil {
		return err
	}

	writeRecords(w, result)
	return nil
}

func getWithLimitAndOffset(table string, limitAndOffset string, d *DbExplorer, w http.ResponseWriter) error {
	limit, err := extractLimitOrOffset(limitAndOffset, "limit", " LIMIT %d")
	if err != nil {
		return err
	}
	offset, err := extractLimitOrOffset(limitAndOffset, "offset", " OFFSET %d")
	if err != nil {
		return err
	}

	rs, err := d.DB.Query(fmt.Sprintf(d.LimitOffsetQuery, table, limit, offset))
	if err != nil {
		return err
	}
	result, err := executeGetQuery(rs, d.Data[table])
	if err != nil {
		return err
	}
	rs.Close()

	writeRecords(w, result)
	return nil
}

func extractLimitOrOffset(limitAndOffset string, targetName string, resultTemplate string) (string, error) {
	var targetValue int
	targetIndex := strings.Index(limitAndOffset, targetName)
	if targetIndex == -1 {
		return "", nil
	}
	cutTarget := limitAndOffset[len(targetName)+1:]
	ampersandIndex := strings.Index(cutTarget, "&")
	if ampersandIndex == -1 {
		var err error
		targetValue, err = strconv.Atoi(cutTarget)
		if err != nil {
			return "", err
		}
	} else {
		var err error
		targetValue, err = strconv.Atoi(cutTarget[:ampersandIndex])
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf(resultTemplate, targetValue), nil
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

	limitOffset, err := regexp.Compile("^(limit=\\d|offset=\\d){1}(&limit=\\d|&offset=\\d)?$")
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
		GetQuery:          "SELECT `%s` FROM `%s`",
		LimitOffsetQuery:  "SELECT `%s` FROM `%s` `%s` `%s`",
		GetByIdQuery:      "SELECT `%s` FROM `%s` WHERE `%s` = ?",
	}, nil
}

func writeTables(w http.ResponseWriter, tables []string) {
	response := struct {
		Tables []string `json:"tables"`
	}{tables}
	writeResponse(w, response)
}

func writeError(w http.ResponseWriter, error string, statusCode int) {
	http.Error(w, error, statusCode)
}

func writeRecords(w http.ResponseWriter, records []map[string]interface{}) {
	response := struct {
		Tables []map[string]interface{} `json:"records"`
	}{records}
	writeResponse(w, response)
}

func writeResponse(w http.ResponseWriter, response interface{}) {
	bytes, err := json.Marshal(struct {
		Response interface{} `json:"response"`
	}{response})
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

func extractFieldNames(tableData []FieldMetaData) []string {
	fieldNames := make([]string, len(tableData))
	for i, datum := range tableData {
		fieldNames[i] = datum.Field
	}
	return fieldNames
}

func convertValue(value string, valueType string) (interface{}, error) {
	if strings.Contains(valueType, "int") {
		atoi, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		return atoi, nil
	}
	if strings.Contains(valueType, "float") {
		atoi, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, err
		}
		return atoi, nil
	}

	return value, nil
}

func executeGetQuery(rs *sql.Rows, tableData []FieldMetaData) ([]map[string]interface{}, error) {
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
			return nil, err
		}

		for i := range convertedRs {
			if convertedRs[i] != nil {
				value, err := convertValue(string(convertedRs[i].([]byte)), tableData[i].Type)
				if err != nil {
					return nil, err
				}
				convertedRs[i] = value
			} else {
				convertedRs[i] = nil
			}
		}

		resultMap := make(map[string]interface{}, countOfFields)
		for i, row := range convertedRs {
			resultMap[tableData[i].Field] = row
		}
		result = append(result, resultMap)

		fmt.Sprintf("")
	}

	return result, nil
}
