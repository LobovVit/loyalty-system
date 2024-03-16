package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/LobovVit/loyalty-system/internal/config"
	"github.com/LobovVit/loyalty-system/internal/domain"
	"github.com/LobovVit/loyalty-system/internal/domain/dbstorage/pgtransactions"
	"github.com/LobovVit/loyalty-system/pkg/logger"
	"github.com/LobovVit/loyalty-system/pkg/retry"
	"github.com/LobovVit/loyalty-system/pkg/security"
	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type OrderUploadedCurrUserErr string
type OrderUploadedAnotherUserErr string
type OrderAcceptedErr string
type UnexpectedErr string
type OrderFormatErr string
type NotExistsErr string
type InsufficientFoundsErr string

func (e OrderUploadedCurrUserErr) Error() string    { return string(e) }
func (e OrderUploadedAnotherUserErr) Error() string { return string(e) }
func (e OrderAcceptedErr) Error() string            { return string(e) }
func (e UnexpectedErr) Error() string               { return string(e) }
func (e OrderFormatErr) Error() string              { return string(e) }
func (e NotExistsErr) Error() string                { return string(e) }
func (e InsufficientFoundsErr) Error() string       { return string(e) }

const OrderUploadedCurrUser OrderUploadedCurrUserErr = "the order number has already been uploaded by this user"
const OrderUploadedAnotherUser OrderUploadedAnotherUserErr = "the order number has already been uploaded by another user"
const OrderAccepted OrderAcceptedErr = "the new order number has been accepted for processing"
const UnexpectedReturn UnexpectedErr = "unexpected error"
const OrderFormat OrderFormatErr = "incorrect order number format"
const NotExists NotExistsErr = "no transactionStorage"
const InsufficientFounds InsufficientFoundsErr = "there are insufficient funds in the account"

type TransactionRepo struct {
	transactionStorage transactionStorage
	balanceRWMutex     sync.RWMutex
	client             *resty.Client
}

type transactionStorage interface {
	AddOrder(ctx context.Context, order *domain.Order) error
	GetOrder(ctx context.Context, orderNumber *string) (*domain.Order, error)
	AddWithdraw(ctx context.Context, withdraw *domain.Withdraw) error
	GetWithdraw(ctx context.Context, orderNumber *string) (*domain.Withdraw, error)
	GetAllOrders(ctx context.Context, UserID *int64) (*[]domain.Order, error)
	GetAllWithdraw(ctx context.Context, UserID *int64) (*[]domain.Withdraw, error)
	GetBalance(ctx context.Context, UserID *int64) (*domain.Balance, error)
	GetUnprocessedOrders(ctx context.Context, batchLimit *int) (*[]domain.Order, error)
	SetProcessedAccruals(ctx context.Context, orders *[]domain.Accrual) error
	IsRetryable(err error) bool
}

func GetTransactionRepo(ctx context.Context, config *config.Config) (TransactionRepo, error) {
	storage, err := pgtransactions.NewOrdersStorage(ctx, config.DSN)
	if err != nil {
		return TransactionRepo{}, err
	}
	return TransactionRepo{
		transactionStorage: storage,
		client: resty.New().
			SetBaseURL(config.AccrualHost).
			SetRetryCount(3).
			SetTimeout(time.Second * 3),
	}, nil
}

func (o *TransactionRepo) NewOrder(ctx context.Context, userID int64, orderNum string) error {
	orderInt, err := strconv.ParseInt(orderNum, 10, 64)
	if err != nil || !security.ValidLuhn(orderInt) {
		return OrderFormat
	}
	order, err := retry.DoWithReturn(ctx, 3, o.transactionStorage.GetOrder, &orderNum, o.transactionStorage.IsRetryable)
	switch {
	case err != nil:
		return fmt.Errorf("get order: %w", err)
	case order == nil:
		amount := domain.CustomMoney(0)
		order = &domain.Order{UserID: userID, Number: orderNum, Status: "NEW", Accrual: &amount, UploadedAt: domain.CustomTime(time.Now())}
		err = retry.DoWithoutReturn(ctx, 3, o.transactionStorage.AddOrder, order, o.transactionStorage.IsRetryable)
		if err != nil {
			return fmt.Errorf("add order: %w", err)
		}
		return OrderAccepted
	case order.UserID == userID:
		return OrderUploadedCurrUser
	case order.UserID != userID:
		return OrderUploadedAnotherUser
	default:
		return UnexpectedReturn
	}
}

func (o *TransactionRepo) GetAllOrders(ctx context.Context, UserID int64) (*[]domain.Order, error) {
	ret, err := retry.DoWithReturn(ctx, 3, o.transactionStorage.GetAllOrders, &UserID, o.transactionStorage.IsRetryable)
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, NotExists
	}
	return ret, nil
}

func (o *TransactionRepo) GetBalance(ctx context.Context, UserID int64) (*domain.Balance, error) {
	o.balanceRWMutex.RLock()
	defer o.balanceRWMutex.RUnlock()
	return retry.DoWithReturn(ctx, 3, o.transactionStorage.GetBalance, &UserID, o.transactionStorage.IsRetryable)
}

func (o *TransactionRepo) GetAllWithdraw(ctx context.Context, UserID int64) (*[]domain.Withdraw, error) {
	ret, err := retry.DoWithReturn(ctx, 3, o.transactionStorage.GetAllWithdraw, &UserID, o.transactionStorage.IsRetryable)
	if err != nil {
		return nil, fmt.Errorf("get all withdraw: %w", err)
	}
	if ret == nil {
		return nil, NotExists
	}
	return ret, nil
}

func (o *TransactionRepo) NewWithdraw(ctx context.Context, newWithdraw domain.Withdraw) error {
	orderInt, err := strconv.ParseInt(newWithdraw.Order, 10, 64)
	if err != nil || !security.ValidLuhn(orderInt) {
		return OrderFormat
	}
	withdraw, err := retry.DoWithReturn(ctx, 3, o.transactionStorage.GetWithdraw, &newWithdraw.Order, o.transactionStorage.IsRetryable)
	switch {
	case err != nil:
		return fmt.Errorf("get withdraw: %w", err)
	case withdraw == nil:
		withdraw = &domain.Withdraw{UserID: newWithdraw.UserID, Order: newWithdraw.Order, Sum: newWithdraw.Sum, ProcessedAt: domain.CustomTime(time.Now())}
		o.balanceRWMutex.Lock()
		defer o.balanceRWMutex.Unlock()
		bal, err := retry.DoWithReturn(ctx, 3, o.transactionStorage.GetBalance, &newWithdraw.UserID, o.transactionStorage.IsRetryable)
		if err != nil {
			return fmt.Errorf("chek balance: %w", err)
		}
		if bal.Current < newWithdraw.Sum {
			return InsufficientFounds
		}
		err = retry.DoWithoutReturn(ctx, 3, o.transactionStorage.AddWithdraw, withdraw, o.transactionStorage.IsRetryable)
		if err != nil {
			return fmt.Errorf("add withdraw: %w", err)
		}
		return nil
	case withdraw.UserID == newWithdraw.UserID:
		return OrderUploadedCurrUser
	case withdraw.UserID != newWithdraw.UserID:
		return OrderUploadedAnotherUser
	default:
		return UnexpectedReturn
	}
}

func (o *TransactionRepo) getAccrual(ctx context.Context, orderNumber *string) (*domain.Accrual, *int, error) {
	ret, err := o.client.
		R().
		SetContext(ctx).
		SetHeader("Content-Type", "text/plain").
		Get(fmt.Sprintf("%v/api/orders/%v", o.client.BaseURL, *orderNumber))
	if err != nil {
		return nil, nil, fmt.Errorf("send: %w", err)
	}
	switch ret.StatusCode() {
	case http.StatusInternalServerError:
		return nil, nil, fmt.Errorf("get accrual: %v", ret.Status())
	case http.StatusNoContent:
		return nil, nil, nil
	case http.StatusTooManyRequests:
		pause, err := strconv.Atoi(ret.Header().Get("Retry-After"))
		if err != nil {
			return nil, nil, nil
		}
		return nil, &pause, nil
	case http.StatusOK:
		accrual := domain.Accrual{}
		err = json.Unmarshal(ret.Body(), &accrual)
		if err != nil {
			return nil, nil, fmt.Errorf("unmarshal json: %w", err)
		}
		if accrual.Sum == nil {
			amount := domain.CustomMoney(0)
			accrual.Sum = &amount
		}
		return &accrual, nil, nil
	}
	return nil, nil, UnexpectedReturn
}

func (o *TransactionRepo) getUnprocessedOrders(ctx context.Context, batchLimit int) (*[]domain.Order, error) {
	ret, err := retry.DoWithReturn(ctx, 3, o.transactionStorage.GetUnprocessedOrders, &batchLimit, o.transactionStorage.IsRetryable)
	if err != nil {
		return nil, fmt.Errorf("get unprocessed: %w", err)
	}
	if ret == nil {
		return nil, NotExists
	}
	return ret, nil
}

func (o *TransactionRepo) setProcessedAccruals(ctx context.Context, accrual *[]domain.Accrual) error {
	o.balanceRWMutex.RLock()
	defer o.balanceRWMutex.RUnlock()
	return retry.DoWithoutReturn(ctx, 3, o.transactionStorage.SetProcessedAccruals, accrual, o.transactionStorage.IsRetryable)
}

func (o *TransactionRepo) processingBatchOrders(ctx context.Context, batchLimit int, SendLimit int) (int, error) {
	pause := 0
	unprocessedOrders, err := o.getUnprocessedOrders(ctx, batchLimit)
	if err != nil {
		logger.Log.Error("Get unprocessed orders", zap.Error(err))
		return pause, fmt.Errorf("get unprocessed orders: %w", err)
	}

	returnedAccrual := make(chan *domain.Accrual, SendLimit)
	requestPause := make(chan *int, SendLimit)
	accrualForSet := make([]domain.Accrual, 0, len(*unprocessedOrders))
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for val := range returnedAccrual {
			accrualForSet = append(accrualForSet, *val)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for val := range requestPause {
			pause = *val
		}
	}()

	g := errgroup.Group{}
	g.SetLimit(SendLimit)

	for _, v := range *unprocessedOrders {
		val := v
		g.Go(func() error {
			accrual, pause, err := o.getAccrual(ctx, &val.Number)
			if err != nil {
				return err
			}
			if pause != nil {
				requestPause <- pause
			}
			if accrual != nil {
				returnedAccrual <- accrual
			}
			return nil
		})
	}
	err = g.Wait()
	if err != nil {
		logger.Log.Error("get accrual", zap.Error(err))
		close(returnedAccrual)
		close(requestPause)
		return pause, fmt.Errorf("get accrual: %w", err)
	}
	close(returnedAccrual)
	close(requestPause)
	wg.Wait()

	err = o.setProcessedAccruals(ctx, &accrualForSet)
	if err != nil {
		logger.Log.Error("Set processed orders", zap.Error(err))
		return pause, fmt.Errorf("set processed orders: %w", err)
	}
	return pause, nil
}

func (o *TransactionRepo) RunProcessing(ctx context.Context, batchLimit int, sendLimit int, pollInterval int) error {
	sendTicker := time.NewTicker(time.Second * time.Duration(pollInterval))
	defer sendTicker.Stop()
	pauseChan := make(chan int)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-sendTicker.C:
				pause, err := o.processingBatchOrders(ctx, batchLimit, sendLimit)
				if err != nil {
					logger.Log.Error("Processing", zap.Error(err))
					return
				}
				if pause > 0 {
					pauseChan <- pause
					return
				}
				logger.Log.Info("Processed")
			case pause := <-pauseChan:
				sendTicker.Stop()
				if pause > pollInterval {
					logger.Log.Info("Requested pause", zap.Int("duration", pause))
					time.Sleep(time.Second * time.Duration(pollInterval-pause))
				}
				sendTicker = time.NewTicker(time.Second * time.Duration(pollInterval))
				logger.Log.Info("Pause")
			case <-ctx.Done():
				logger.Log.Info("Processing shutting down gracefully")
				return
			}
		}
	}()
	wg.Wait()
	return nil
}