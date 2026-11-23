package generics

import (
	"fmt"
)

// TODO: Map должен применить transform к каждому элементу и вернуть новый []R.
// Порядок сохраняется; nil и пустой вход дают пустой ненулевой слайс.
func Map[T any, R any](values []T, transform func(T) R) []R {
	if values == nil || len(values) == 0 {
		return []R{}
	}
	result := make([]R, 0, len(values))
	for _, value := range values {
		result = append(result, transform(value))
	}
	return result
}

// TODO: Unique должен оставить первое вхождение каждого значения и сохранить порядок.
// T ограничен comparable, поэтому значения можно использовать как ключи map.
func Unique[T comparable](values []T) []T {
	seen := make(map[T]struct{})
	result := make([]T, 0, len(values))

	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		result = append(result, value)
		seen[value] = struct{}{}
	}
	return result
}

// TODO: Drain (прочитать всё) должен получить значения до закрытия канала
// и сохранить их порядок. Nil-канал не нужно читать: для него верни
// пустой ненулевой слайс.
func Drain[T any](values <-chan T) []T {
	result := make([]T, 0)

	if values == nil {
		return result
	}

	for value := range values {
		result = append(result, value)
	}

	return result
}

// TODO: Filter должен вернуть новый слайс только со значениями, для которых
// keep вернул true. При keep == nil вернуть независимую копию values.
func Filter[T any](values []T, keep func(T) bool) []T {
	result := make([]T, 0, len(values))
	if keep == nil {
		copyValues := make([]T, len(values))
		copy(copyValues, values)
		return copyValues
	}
	for _, value := range values {
		if keep(value) {
			result = append(result, value)
		}
	}
	return result
}

// TODO: IndexOf должен вернуть индекс первого target или -1, если target нет.
// T ограничен comparable, потому что значения сравниваются через ==.
func IndexOf[T comparable](values []T, target T) int {
	for i, value := range values {
		if value == target {
			return i
		}
	}
	return -1
}

func Example() string {
	labels := Map([]int{1, 2, 3}, func(value int) string {
		return fmt.Sprintf("item-%d", value)
	})

	words := make(chan string, 2)
	words <- "go"
	words <- "generic"
	close(words)

	return fmt.Sprintf("map=%v\nunique=%v\ndrain=%v", labels, Unique([]int{2, 1, 2, 3, 1}), Drain(words))
}
