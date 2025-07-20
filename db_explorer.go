package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
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
	Collation  sql.NullString
	Null       sql.NullString
	Key        sql.NullString
	Default    sql.NullString
	Extra      sql.NullString
	Privileges sql.NullString
	Comment    sql.NullString
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
	InsertQuery       string
	UpdateQuery       string
	DeleteQuery       string
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
		err := getWithLimitAndOffset(tableName, queryParams, d, w)
		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
		}
	}

	if method == http.MethodGet && d.ByIdRegexp.MatchString(afterTable) {
		err := getById(tableName, d, w, afterTable)
		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
		}
	}

	if method == http.MethodPut {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
		}
		r.Body.Close()
		createRow(tableName, d, w, body)
	}

	if method == http.MethodPost && d.ByIdRegexp.MatchString(afterTable) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
		}
		r.Body.Close()
		updateRow(tableName, d, w, body, afterTable)
	}

	if method == http.MethodDelete && d.ByIdRegexp.MatchString(afterTable) {
		deleteRow(tableName, d, w, afterTable)
	}
}

func deleteRow(table string, d *DbExplorer, w http.ResponseWriter, restOfPath string) error {
	idForDelete, err := strconv.Atoi(restOfPath[1:])
	if err != nil {
		return err
	}

	idFieldName := getPrimaryKey(table, d)

	query := fmt.Sprintf(d.DeleteQuery, table, idFieldName)
	_, err = d.DB.Query(query, idForDelete)
	if err != nil {
		return err
	}

	res, err := d.DB.Query("SELECT ROW_COUNT()")
	if err != nil {
		return err
	}
	res.Next()
	var deleted int
	res.Scan(&deleted)

	writeResponse(w, struct {
		Updated int `json:"deleted"`
	}{deleted})
	return nil
}

func updateRow(table string, d *DbExplorer, w http.ResponseWriter, body []byte, restOfPath string) error {
	input := make(map[string]interface{})
	err := json.Unmarshal(body, &input)
	if err != nil {
		return err
	}

	idForUpdate, err := strconv.Atoi(restOfPath[1:])
	if err != nil {
		return err
	}

	fieldNames, fieldValues, idFieldName := createDataForQuery(d.Data[table], input)

	updateExpression := make([]string, len(fieldNames))
	for i := range fieldNames {
		updateExpression[i] = fmt.Sprintf("%s = %s", fieldNames[i], fieldValues[i])
	}

	query := fmt.Sprintf(d.UpdateQuery,
		table,
		strings.Join(updateExpression, ", "),
		idFieldName)

	_, err = d.DB.Query(query, idForUpdate)
	if err != nil {
		return err
	}

	res, err := d.DB.Query("SELECT ROW_COUNT()")
	if err != nil {
		return err
	}
	res.Next()
	var updated int
	res.Scan(&updated)

	writeResponse(w, struct {
		Updated int `json:"updated"`
	}{updated})
	return nil
}

func createRow(table string, d *DbExplorer, w http.ResponseWriter, body []byte) error {
	input := make(map[string]interface{})
	err := json.Unmarshal(body, &input)
	if err != nil {
		return err
	}

	forInsertFieldNames, forInsertFieldValues, idFieldName := createDataForQuery(d.Data[table], input)

	query := fmt.Sprintf(d.InsertQuery,
		table,
		strings.Join(forInsertFieldNames, "`, `"),
		strings.Join(forInsertFieldValues, ", "),
		idFieldName)
	rs, err := d.DB.Query(query)
	if err != nil {
		return err
	}

	var resultId int
	rs.Next()
	err = rs.Scan(&resultId)
	if err != nil {
		return err
	}

	writeResponse(w, struct {
		Id int `json:"id"`
	}{resultId})

	return nil
}

func createDataForQuery(data []FieldMetaData, input map[string]interface{}) ([]string, []string, string) {
	forInsertFieldNames := make([]string, 0)
	forInsertFieldValues := make([]string, 0)
	var targetName string
	for _, datum := range data {
		value, exists := input[datum.Field]
		if exists && datum.Extra.String != "auto_increment" {
			forInsertFieldNames = append(forInsertFieldNames, datum.Field)
			if strings.Contains(datum.Type, "text") || strings.Contains(datum.Type, "var") {
				forInsertFieldValues = append(forInsertFieldValues, fmt.Sprintf("'%v'", value))
			} else {
				forInsertFieldValues = append(forInsertFieldValues, fmt.Sprintf("%v", value))
			}
		}
		if datum.Key.String == "PRI" {
			targetName = datum.Field
		}
	}
	return forInsertFieldNames, forInsertFieldValues, targetName
}

func getById(table string, d *DbExplorer, w http.ResponseWriter, restOfPath string) error {
	id, err := strconv.Atoi(restOfPath[1:])
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
	}

	idFieeldName := getPrimaryKey(table, d)

	rs, err := d.DB.Query(fmt.Sprintf(d.GetByIdQuery, getAndFormatFieldNamesForQuery(table, d), table, idFieeldName), id)
	if err != nil {
		return err
	}
	result, err := processRs(rs, d.Data[table])
	rs.Close()
	if err != nil {
		return err
	}

	if len(result) == 0 {
		writeError(w, "record not found", http.StatusNotFound)
		return nil
	}

	writeRecord(w, result[0])
	return nil
}

func getPrimaryKey(table string, d *DbExplorer) string {
	var idFieeldName string
	for _, data := range d.Data[table] {
		if data.Key.String == "PRI" {
			idFieeldName = data.Field
			break
		}
	}
	return idFieeldName
}

func getRows(table string, d *DbExplorer, w http.ResponseWriter) error {
	rs, err := d.DB.Query(fmt.Sprintf(d.GetQuery, getAndFormatFieldNamesForQuery(table, d), table))
	if err != nil {
		return err
	}
	result, err := processRs(rs, d.Data[table])
	rs.Close()
	if err != nil {
		return err
	}

	writeRecords(w, result)
	return nil
}

func getWithLimitAndOffset(table string, limitAndOffset string, d *DbExplorer, w http.ResponseWriter) error {
	limit, err := extractLimitOrOffset(limitAndOffset, "limit", 5)
	if err != nil {
		return err
	}
	offset, err := extractLimitOrOffset(limitAndOffset, "offset", 0)
	if err != nil {
		return err
	}

	fieldNames := getAndFormatFieldNamesForQuery(table, d)
	fullQuery := fmt.Sprintf(d.LimitOffsetQuery, fieldNames, table, limit, offset)
	rs, err := d.DB.Query(fullQuery)
	if err != nil {
		return err
	}
	result, err := processRs(rs, d.Data[table])
	if err != nil {
		return err
	}
	rs.Close()

	writeRecords(w, result)
	return nil
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
			err := fieldsRs.Scan(
				&field.Field, &field.Type, &field.Collation,
				&field.Null, &field.Key, &field.Default,
				&field.Extra, &field.Privileges, &field.Comment,
			)
			if err != nil {
				return nil, err
			}
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
		LimitOffsetQuery:  "SELECT `%s` FROM `%s` %s %s",
		GetByIdQuery:      "SELECT `%s` FROM `%s` WHERE `%s` = ?",
		InsertQuery:       "INSERT INTO `%s` (`%s`) VALUES (%s) RETURNING `%s`",
		UpdateQuery:       "UPDATE `%s` SET %s WHERE `%s` = ?",
		DeleteQuery:       "DELETE FROM `%s` WHERE `%s` = ?",
	}, nil
}

func writeTables(w http.ResponseWriter, tables []string) {
	response := struct {
		Tables []string `json:"tables"`
	}{tables}
	writeResponse(w, response)
}

func writeError(w http.ResponseWriter, error string, statusCode int) {
	marshal, _ := json.Marshal(struct {
		Error string `json:"error"`
	}{error})
	http.Error(w, string(marshal), statusCode)
}

func writeRecords(w http.ResponseWriter, records []map[string]interface{}) {
	response := struct {
		Tables []map[string]interface{} `json:"records"`
	}{records}
	writeResponse(w, response)
}

func writeRecord(w http.ResponseWriter, record map[string]interface{}) {
	response := struct {
		Tables map[string]interface{} `json:"record"`
	}{record}
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

func extractLimitOrOffset(limitAndOffset string, targetName string, defaultValue int) (string, error) {
	var targetValue int
	resultTemplate := fmt.Sprintf(" %s %%d", strings.ToUpper(targetName))
	targetIndex := strings.Index(limitAndOffset, targetName)
	if targetIndex == -1 {
		return fmt.Sprintf(resultTemplate, defaultValue), nil
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

func processRs(rs *sql.Rows, tableData []FieldMetaData) ([]map[string]interface{}, error) {
	countOfFields := len(tableData)
	result := make([]map[string]interface{}, 0)
	for rs.Next() {
		convertedRs := make([]interface{}, countOfFields)
		unconvertedRs := make([][]byte, countOfFields)
		convertedRsPointers := make([]interface{}, countOfFields)
		for i := range convertedRsPointers {
			convertedRsPointers[i] = &unconvertedRs[i]
		}
		err := rs.Scan(convertedRsPointers...)
		if err != nil {
			return nil, err
		}

		for i := range unconvertedRs {
			if unconvertedRs[i] != nil {
				value, err := convertValue(string(unconvertedRs[i]), tableData[i].Type)
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

func getAndFormatFieldNamesForQuery(table string, d *DbExplorer) string {
	return strings.Join(extractFieldNames(d.Data[table]), "`, `")
}
