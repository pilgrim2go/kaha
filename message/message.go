package message

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Message represents kev-value format like JSON
type Message map[string]interface{}

// RemoveFields removes root fields by name
func (m Message) RemoveFields(fields []string) {
	for _, field := range fields {
		delete(m, field)
	}
}

// RemoveEmptyFields removes root empty fields
func (m Message) RemoveEmptyFields() {
	for name, value := range m {
		if value == nil {
			delete(m, name)
		}
	}
}

// ReduceToFields leaves only given fields and return removed ones
func (m Message) ReduceToFields(onlyFields []string) map[string]interface{} {
	reduced := make(map[string]interface{})
	for name, value := range m {
		if !strInSlice(name, onlyFields) {
			delete(m, name)
			reduced[name] = value
		}
	}
	return reduced
}

// RenameFields renames field's name in given path
func (m Message) RenameFields(pathName map[string]string) {
	for path, name := range pathName {
		if value, ok := getValueRemovePath(strings.Split(path, "."), m); ok {
			m[name] = value
		}
	}
}

// SubMatchValues changes field's value based on regexp submatch
func (m Message) SubMatchValues(nameRegexp map[string]*regexp.Regexp) error {
	for name, rgxp := range nameRegexp {
		if rgxp == nil {
			return fmt.Errorf("field: %s regexp could not be nil", name)
		}

		if value, ok := m[name]; ok {
			switch v := value.(type) {
			case int:
				str := strconv.Itoa(v)
				substr := strings.Join(rgxp.FindStringSubmatch(str), "")
				if i, err := strconv.Atoi(substr); err == nil {
					m[name] = i
				} else {
					return fmt.Errorf("field: %s value: %s is not convertable to int", name, substr)
				}
			case string:
				m[name] = strings.Join(rgxp.FindStringSubmatch(v), "")
			default:
				return fmt.Errorf("field: %s value %v is not convertable to string or int", name, v)
			}
		}
	}
	return nil
}

func strInSlice(str string, list []string) bool {
	for _, v := range list {
		if v == str {
			return true
		}
	}
	return false
}

func getValueRemovePath(path []string, data map[string]interface{}) (interface{}, bool) {
	if len(path) == 0 {
		return data, true
	}

	item := path[0]
	if value, ok := data[item]; ok {
		if len(path) >= 2 {
			if v, ok := value.(map[string]interface{}); ok {
				return getValueRemovePath(path[1:], v)
			}
			return nil, false
		}
		delete(data, item)
		return value, true
	}
	return nil, false
}
