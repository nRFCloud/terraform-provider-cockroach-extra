package resources

import (
	"github.com/jackc/pgx"
	"net/url"
	"sort"
	"strings"
)

func revokeAllPrivileges(db *pgx.ConnPool, principal string) error {
	rows, err := db.Query("select database_name from [show databases]")
	if err != nil {
		return err
	}
	defer rows.Close()
	var dbNames []string
	for rows.Next() {
		var dbName string
		err = rows.Scan(&dbName)
		if err != nil {
			return err
		}
		if dbName != "system" && dbName != "postgres" {
			dbNames = append(dbNames, dbName)
		}
	}

	for _, dbName := range dbNames {
		_, err = db.Exec("REVOKE ALL ON " + pgx.Identifier{dbName}.Sanitize() + ".* FROM " + pgx.Identifier{principal}.Sanitize())
		if err != nil {
			if strings.Contains(err.Error(), "no object matched") {
				// This means that the database has nothing in it
				continue
			}
			return err
		}
	}
	return nil
}

func CompareURLs(url1, url2 string) bool {
	parsedUrl1, err1 := url.Parse(url1)
	parsedUrl2, err2 := url.Parse(url2)

	if err1 != nil || err2 != nil {
		return false
	}

	if parsedUrl1.Scheme != parsedUrl2.Scheme ||
		parsedUrl1.Host != parsedUrl2.Host ||
		parsedUrl1.Path != parsedUrl2.Path {
		return false
	}

	params1 := parsedUrl1.Query()
	params2 := parsedUrl2.Query()

	var redactedSet []string

	// remove 'redacted' query params
	for key, value := range params1 {
		if strings.ToLower(value[0]) == "redacted" {
			redactedSet = append(redactedSet, key)
		}
	}

	for key, value := range params2 {
		if strings.ToLower(value[0]) == "redacted" {
			redactedSet = append(redactedSet, key)
		}
	}

	for _, key := range redactedSet {
		params1.Del(key)
		params2.Del(key)
	}

	// sort and compare query params
	if len(params1) != len(params2) {
		return false
	}

	keys1 := make([]string, len(params1))
	keys2 := make([]string, len(params2))

	i := 0
	for key := range params1 {
		keys1[i] = key
		i++
	}

	i = 0
	for key := range params2 {
		keys2[i] = key
		i++
	}

	sort.Strings(keys1)
	sort.Strings(keys2)

	for i := range keys1 {
		if keys1[i] != keys2[i] {
			return false
		}
		if params1.Get(keys1[i]) != params2.Get(keys2[i]) {
			return false
		}
	}

	return true
}

func SanatizeValue(value string) string {
	return strings.Replace(pgx.Identifier{value}.Sanitize(), "\"", "'", -1)
}
