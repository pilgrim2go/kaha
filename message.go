package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type Message map[string]interface{}

func (m Message) RemoveFields(fields []string) {
	for _, field := range fields {
		delete(m, field)
	}
}

func (m Message) RemoveEmptyFields() {
	for name, value := range m {
		if value == nil {
			delete(m, name)
		}
	}
}

func (m Message) ReduceFields(onlyFields []string) map[string]interface{} {
	reduced := make(map[string]interface{})
	for name, value := range m {
		if !strInSlice(name, onlyFields) {
			delete(m, name)
			reduced[name] = value
		}
	}
	return reduced
}

func (m Message) FlatFields(pathName map[string]string) {
	for path, name := range pathName {
		if value, ok := getValueRemovePath(strings.Split(path, "."), m); ok {
			m[name] = value
		}
	}
}

func (m Message) SubMatchValues(fieldRegexp map[string]string) error {
	for field, strgxp := range fieldRegexp {
		if value, ok := m[field]; ok {
			rgx := regexp.MustCompile(strgxp)

			switch v := value.(type) {
			case int:
				str := strconv.Itoa(v)
				substr := strings.Join(rgx.FindStringSubmatch(str), "")
				if i, err := strconv.Atoi(substr); err == nil {
					m[field] = i
				} else {
					return fmt.Errorf("Field: %s value: %s is not convertable to int", field, substr)
				}
			case string:
				m[field] = rgx.FindStringSubmatch(v)
			default:
				return fmt.Errorf("Field: %s value %v is not convertable to string or int", field, v)
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
