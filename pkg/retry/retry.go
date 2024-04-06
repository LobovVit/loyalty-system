package retry

import (
	"context"
	"time"
)

func DoWithoutReturn[T any](ctx context.Context, repeat int, retryFunc func(context.Context, T) error, p T, isRepeatableFunc func(err error) bool) error {
	var err error
	for i := 0; i < repeat; i++ {
		// Return immediately if ctx is canceled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err = retryFunc(ctx, p)
		if err == nil || !isRepeatableFunc(err) {
			break
		}
		if i < repeat-1 {
			time.Sleep(time.Second * 3)
		}
	}
	return err
}

func DoWithReturn[T, R any](ctx context.Context, repeat int, retryFunc func(context.Context, T) (R, error), p T, isRepeatableFunc func(err error) bool) (R, error) {
	var err error
	var ret R
	for i := 0; i < repeat; i++ {
		// Return immediately if ctx is canceled
		select {
		case <-ctx.Done():
			return ret, ctx.Err()
		default:
		}

		ret, err = retryFunc(ctx, p)
		if err == nil || !isRepeatableFunc(err) {
			break
		}
		if i < repeat-1 {
			time.Sleep(time.Second * 3)
		}
	}
	return ret, err
}
