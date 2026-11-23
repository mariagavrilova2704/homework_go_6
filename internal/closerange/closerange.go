package closerange

import (
	"fmt"
	"sync"
)

// TODO: Generate должен в отдельной горутине отправить числа от 0 до limit-1,
// затем закрыть канал. Неположительный limit даёт сразу закрытый канал.
func Generate(limit int) <-chan int {
	values := make(chan int)
	go func() {
		defer close(values)
		for i := 0; i < limit; i++ {
			values <- i
		}
	}()

	return values
}

// TODO: Collect должен прочитать канал циклом range до закрытия.
// Если отправитель не закроет канал, цикл будет ждать следующее значение.
// nil-канал трактуется как пустой вход; результат всегда ненулевой слайс.
func Collect(values <-chan int) []int {
	collection := make([]int, 0)
	if values == nil {
		return collection
	}
	for value := range values {
		collection = append(collection, value)
	}
	return collection
}

// TODO: FilterEven должен отправить только чётные значения в исходном порядке
// и закрыть выходной канал после завершения.
func FilterEven(values []int) <-chan int {
	result := make(chan int)
	go func() {
		defer close(result)
		for _, value := range values {
			if value%2 == 0 {
				result <- value
			}
		}
	}()
	return result
}

// TODO: Double должен в отдельной горутине отправить удвоенные значения
// в исходном порядке и закрыть выходной канал даже для пустого входа.
func Double(values []int) <-chan int {
	result := make(chan int)
	go func() {
		defer close(result)

		for _, value := range values {
			result <- value * 2
		}
	}()
	return result
}

// TODO: Merge должен объединить два входных канала в один и закрыть выход
// только после завершения обоих входов. nil-канал трактуется как пустой вход.
// Порядок внутри каждого входа сохраняется, общий порядок между входами не задан.
func Merge(left, right <-chan int) <-chan int {
	result := make(chan int)
	var wg sync.WaitGroup

	if left != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for leftvalues := range left {
				result <- leftvalues
			}
		}()
	}
	if right != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rightvalues := range right {
				result <- rightvalues
			}
		}()
	}

	go func() {
		wg.Wait()
		close(result)
	}()
	return result
}

func Example() string {
	return fmt.Sprintf("generated=%v\neven=%v", Collect(Generate(5)), Collect(FilterEven([]int{1, 2, 3, 4, 5, 6})))
}
