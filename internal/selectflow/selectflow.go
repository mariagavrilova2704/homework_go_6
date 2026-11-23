package selectflow

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrTimeout = errors.New("истекло время ожидания")
	ErrClosed  = errors.New("канал закрыт")
)

// TODO: FirstReady должен дождаться готового канала и вернуть значение,
// название источника и true. Источник равен "left" или "right".
// Закрытый выбранный канал даёт пустую строку, название источника и false.
func FirstReady(left, right <-chan string) (string, string, bool) {
	select {
	case value, ok := <-left:
		return value, "left", ok
	case value, ok := <-right:
		return value, "right", ok
	}
}

// TODO: TrySend должен попытаться отправить value без ожидания.
// Если канал не готов принять значение, ветка default сразу возвращает false.
func TrySend(out chan<- int, value int) bool {
	select {
	case out <- value:
		return true
	default:
		return false
	}
}

// TODO: ReceiveOrTimeout должен вернуть значение из in, ErrClosed для закрытого in
// или ErrTimeout, когда первым готов канал тайм-аута.
func ReceiveOrTimeout(in <-chan string, timeout <-chan time.Time) (string, error) {
	select {
	case value, ok := <-in:
		if !ok {
			return "", ErrClosed
		}
		return value, nil
	case <-timeout:
		return "", ErrTimeout
	}
}

// TODO: TryReceive должен попытаться получить одно число без ожидания.
// Вернуть value, true только для реально полученного значения; nil, пустой
// или закрытый канал дают 0, false.
func TryReceive(in <-chan int) (int, bool) {
	select {
	case value, ok := <-in:
		if !ok {
			return 0, false
		}
		return value, true

	default:
		return 0, false
	}
}

// TODO: PrimaryOrFallback должен сначала проверить основной канал primary без ожидания.
// Если он не готов, нужно ждать значение из основного или запасного канала.
// В сигнатуре запасной канал называется fallback.
// Закрытый вход отключается; когда оба входа закрыты или равны nil,
// вернуть "", "", false.
func PrimaryOrFallback(primary, fallback <-chan string) (string, string, bool) {
	for primary != nil || fallback != nil {
		select {
		case value, ok := <-primary:
			if !ok {
				primary = nil
				continue
			}
			return value, "primary", true
		default:
		}

		select {
		case value, ok := <-primary:
			if !ok {
				primary = nil
			} else {
				return value, "primary", true
			}
		case value, ok := <-fallback:
			if !ok {
				fallback = nil
			} else {
				return value, "fallback", true
			}
		}
	}
	return "", "", false
}

func Example() string {
	left := make(chan string, 1)
	left <- "left-value"
	value, source, ok := FirstReady(left, nil)
	out := make(chan int, 1)
	sent := TrySend(out, 7)
	ready := make(chan string, 1)
	ready <- "ready"
	received, _ := ReceiveOrTimeout(ready, nil)
	return fmt.Sprintf("first=%s:%s:%t\nsend=%t\nreceive=%s", source, value, ok, sent, received)
}
