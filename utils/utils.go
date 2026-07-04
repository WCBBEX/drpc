package utils

import (
	"errors"
	"strings"
)

func CopySlice[T any](src []T) []T {
	dst := make([]T, len(src))
	copy(dst, src)
	return dst
}

func GetServiceNameFromServiceMethod(serviceMethod string) (string, error) {
	split := strings.Split(serviceMethod, ".")
	if len(split) != 2 {
		return "", errors.New("wrong format for serviceName")
	}
	return split[0], nil
}
