package sql

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// ParamConverter 参数转换器
type ParamConverter struct {
	timeFormat string
	maxDepth   int
}

// NewParamConverter 创建参数转换器
func NewParamConverter() *ParamConverter {
	return &ParamConverter{
		timeFormat: time.RFC3339Nano,
		maxDepth:   10,
	}
}

// Convert 转换参数
func (c *ParamConverter) Convert(param interface{}) (interface{}, error) {
	return c.convertRecursive(param, 0)
}

// convertRecursive 递归转换参数
func (c *ParamConverter) convertRecursive(param interface{}, depth int) (interface{}, error) {
	if depth > c.maxDepth {
		return nil, fmt.Errorf("maximum recursion depth exceeded")
	}

	if param == nil {
		return nil, nil
	}

	// 检查是否已经是支持的类型
	switch v := param.(type) {
	case []byte:
		return v, nil
	case string:
		return v, nil
	case int, int8, int16, int32, int64:
		return v, nil
	case uint, uint8, uint16, uint32, uint64:
		return v, nil
	case float32, float64:
		return v, nil
	case bool:
		return v, nil
	case time.Time:
		return v, nil
	case *time.Time:
		if v == nil {
			return nil, nil
		}
		return *v, nil
	case driver.Valuer:
		// 数据库驱动支持的类型
		return v, nil
	}

	// 使用反射处理其他类型
	v := reflect.ValueOf(param)

	// 处理指针
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, nil
		}
		return c.convertRecursive(v.Elem().Interface(), depth+1)
	}

	// 处理切片
	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		return c.convertSlice(v, depth)
	}

	// 处理map
	if v.Kind() == reflect.Map {
		return c.convertMap(v, depth)
	}

	// 处理结构体
	if v.Kind() == reflect.Struct {
		return c.convertStruct(v, depth)
	}

	// 默认转换为字符串
	return fmt.Sprintf("%v", param), nil
}

// convertSlice 转换切片
func (c *ParamConverter) convertSlice(v reflect.Value, depth int) (interface{}, error) {
	// 特殊处理 []byte
	if v.Type().Elem().Kind() == reflect.Uint8 {
		return v.Bytes(), nil
	}

	length := v.Len()
	if length == 0 {
		return "{}", nil // PostgreSQL 空数组
	}

	// 构建PostgreSQL数组字符串
	var builder strings.Builder
	builder.WriteString("{")

	for i := 0; i < length; i++ {
		if i > 0 {
			builder.WriteString(",")
		}

		elem := v.Index(i).Interface()
		converted, err := c.convertRecursive(elem, depth+1)
		if err != nil {
			return nil, err
		}

		// 根据类型格式化
		switch val := converted.(type) {
		case string:
			// 转义双引号
			escaped := strings.ReplaceAll(val, "\"", "\"\"")
			builder.WriteString(fmt.Sprintf("\"%s\"", escaped))
		case []byte:
			builder.WriteString(fmt.Sprintf("\"\\\\x%x\"", val))
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64:
			builder.WriteString(fmt.Sprintf("%d", val))
		case float32, float64:
			builder.WriteString(strconv.FormatFloat(reflect.ValueOf(val).Float(), 'f', -1, 64))
		case bool:
			builder.WriteString(strconv.FormatBool(val))
		case time.Time:
			builder.WriteString(fmt.Sprintf("\"%s\"", val.Format(c.timeFormat)))
		default:
			// 尝试转换为字符串
			builder.WriteString(fmt.Sprintf("\"%v\"", val))
		}
	}

	builder.WriteString("}")
	return builder.String(), nil
}

// convertMap 转换map
func (c *ParamConverter) convertMap(v reflect.Value, depth int) (interface{}, error) {
	// 转换为JSON
	data, err := json.Marshal(v.Interface())
	if err != nil {
		return nil, fmt.Errorf("failed to marshal map to JSON: %w", err)
	}
	return data, nil
}

// convertStruct 转换结构体
func (c *ParamConverter) convertStruct(v reflect.Value, depth int) (interface{}, error) {
	// 检查是否实现了driver.Valuer接口
	if valuer, ok := v.Interface().(driver.Valuer); ok {
		return valuer, nil
	}

	// 检查是否是time.Time
	if v.Type().String() == "time.Time" {
		return v.Interface().(time.Time), nil
	}

	// 转换为JSON
	data, err := json.Marshal(v.Interface())
	if err != nil {
		return nil, fmt.Errorf("failed to marshal struct to JSON: %w", err)
	}
	return data, nil
}

// ConvertBatch 批量转换参数
func (c *ParamConverter) ConvertBatch(params []interface{}) ([]interface{}, error) {
	result := make([]interface{}, len(params))

	for i, param := range params {
		converted, err := c.Convert(param)
		if err != nil {
			return nil, fmt.Errorf("failed to convert param at index %d: %w", i, err)
		}
		result[i] = converted
	}

	return result, nil
}

// ValidateParams 验证参数
func (c *ParamConverter) ValidateParams(query string, params []interface{}) error {
	// 统计查询中的占位符数量
	placeholderCount := strings.Count(query, "$")

	// 检查数字占位符
	maxPlaceholder := 0
	for i := 1; i <= 100; i++ {
		if strings.Contains(query, fmt.Sprintf("$%d", i)) {
			maxPlaceholder = i
		}
	}

	// 检查参数数量是否匹配
	expectedParams := max(placeholderCount, maxPlaceholder)
	if len(params) != expectedParams {
		return fmt.Errorf("parameter count mismatch: expected %d, got %d", expectedParams, len(params))
	}

	return nil
}

// PrepareNamedParams 处理命名参数
func (c *ParamConverter) PrepareNamedParams(query string, params map[string]interface{}) (string, []interface{}, error) {
	var resultParams []interface{}
	resultQuery := query

	for key, value := range params {
		placeholder := ":" + key

		// 查找所有命名参数
		for strings.Contains(resultQuery, placeholder) {
			// 转换参数
			converted, err := c.Convert(value)
			if err != nil {
				return "", nil, fmt.Errorf("failed to convert param %s: %w", key, err)
			}

			// 替换占位符
			resultQuery = strings.Replace(resultQuery, placeholder, fmt.Sprintf("$%d", len(resultParams)+1), 1)
			resultParams = append(resultParams, converted)
		}
	}

	return resultQuery, resultParams, nil
}

// ParseArray 解析PostgreSQL数组字符串
func (c *ParamConverter) ParseArray(arrayStr string, elemType reflect.Type) (interface{}, error) {
	if !strings.HasPrefix(arrayStr, "{") || !strings.HasSuffix(arrayStr, "}") {
		return nil, fmt.Errorf("invalid array format: %s", arrayStr)
	}

	// 移除大括号
	content := arrayStr[1 : len(arrayStr)-1]
	if content == "" {
		// 空数组
		return reflect.MakeSlice(reflect.SliceOf(elemType), 0, 0).Interface(), nil
	}

	// 解析数组元素
	var elements []string
	var current strings.Builder
	inQuotes := false
	escaped := false

	for _, ch := range content {
		if escaped {
			current.WriteRune(ch)
			escaped = false
			continue
		}

		if ch == '\\' {
			escaped = true
			continue
		}

		if ch == '"' {
			inQuotes = !inQuotes
			continue
		}

		if !inQuotes && ch == ',' {
			elements = append(elements, current.String())
			current.Reset()
			continue
		}

		current.WriteRune(ch)
	}

	if current.Len() > 0 {
		elements = append(elements, current.String())
	}

	// 转换为目标类型
	slice := reflect.MakeSlice(reflect.SliceOf(elemType), len(elements), len(elements))

	for i, elem := range elements {
		// 去除引号
		if strings.HasPrefix(elem, "\"") && strings.HasSuffix(elem, "\"") {
			elem = elem[1 : len(elem)-1]
			// 处理转义的双引号
			elem = strings.ReplaceAll(elem, "\"\"", "\"")
		}

		// 根据类型转换
		switch elemType.Kind() {
		case reflect.String:
			slice.Index(i).SetString(elem)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			val, err := strconv.ParseInt(elem, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse int: %w", err)
			}
			slice.Index(i).SetInt(val)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			val, err := strconv.ParseUint(elem, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse uint: %w", err)
			}
			slice.Index(i).SetUint(val)
		case reflect.Float32, reflect.Float64:
			val, err := strconv.ParseFloat(elem, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse float: %w", err)
			}
			slice.Index(i).SetFloat(val)
		case reflect.Bool:
			val, err := strconv.ParseBool(elem)
			if err != nil {
				return nil, fmt.Errorf("failed to parse bool: %w", err)
			}
			slice.Index(i).SetBool(val)
		default:
			return nil, fmt.Errorf("unsupported array element type: %v", elemType)
		}
	}

	return slice.Interface(), nil
}

// max 辅助函数
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// SetTimeFormat 设置时间格式
func (c *ParamConverter) SetTimeFormat(format string) {
	c.timeFormat = format
}

// SetMaxDepth 设置最大递归深度
func (c *ParamConverter) SetMaxDepth(depth int) {
	c.maxDepth = depth
}
