package contextflow

import (
	"context"
	"errors"
	"fmt"
)

var ErrClosed = errors.New("канал закрыт")

// TODO: Check должен немедленно вернуть ctx.Err(), если context завершён,
// и nil, если работа ещё разрешена.
func Check(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// TODO: WaitValue должен ждать значение или отмену context через select.
// Закрытый канал возвращает ErrClosed.
func WaitValue(ctx context.Context, values <-chan string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case value, ok := <-values:
		if !ok {
			return "", ErrClosed
		}
		return value, nil
	}
}

// TODO: Process должен перед каждым элементом проверять ctx, затем применять fn.
// При отмене вернуть уже накопленный результат и ctx.Err().
// Если fn == nil, копировать значения без изменения.
func Process(ctx context.Context, values []int, fn func(int) int) ([]int, error) {
	result := make([]int, 0, len(values))
	for _, value := range values {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}
		if fn == nil {
			result = append(result, value)
		} else {
			result = append(result, fn(value))
		}
	}
	return result, nil
}

// TODO: Send должен ждать, пока out примет value, или пока завершится context.
// Успешная отправка возвращает nil, отмена — ctx.Err().
func Send(ctx context.Context, out chan<- int, value int) error {
	// Ненулевая ошибка не даёт стартовому тесту зависнуть на чтении из пустого
	// канала. Ученице всё равно нужно заменить эту строку решением через select.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case out <- value:
		return nil
	}
}

// TODO: CollectN должен собрать ровно count значений, поддерживая отмену.
// При раннем close вернуть частичный результат и ErrClosed; при отмене —
// частичный результат и ctx.Err(). count <= 0 даёт пустой результат без ошибки.
func CollectN(ctx context.Context, values <-chan int, count int) ([]int, error) {
	if count <= 0 {
		return []int{}, nil
	}
	result := make([]int, 0, count)
	for len(result) < count {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case value, ok := <-values:
			if !ok {
				return result, ErrClosed
			}
			result = append(result, value)
		}
	}

	return result, nil
}

func Example() string {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	checkErr := Check(ctx)
	ready := make(chan string, 1)
	ready <- "done"
	value, _ := WaitValue(context.Background(), ready)
	result, _ := Process(context.Background(), []int{1, 2, 3}, func(v int) int { return v * 2 })
	return fmt.Sprintf("check=%v\nvalue=%s\nprocess=%v", checkErr, value, result)
}
