package pgtransactions

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
	"loyalty-system/internal/domain"
	"loyalty-system/pkg/logger"
	"loyalty-system/pkg/postgresql"
)

type PGOrdersStorage struct {
	dbConnections *sql.DB
}

func NewOrdersStorage(ctx context.Context, dsn string) (*PGOrdersStorage, error) {
	dbCon, err := postgresql.NewConn(dsn)
	if err != nil {
		logger.Log.Error("Get db connection failed", zap.Error(err))
		return nil, err
	}
	s := &PGOrdersStorage{dbConnections: dbCon}
	const createTableSQL = `create table IF NOT EXISTS transactions (
    							userid int references users(id) not null, 
    							type text not null, 
    							number text not null ,
    							status text not null, 
    							amount bigint not null default 0, 
    							uploaded_at TIMESTAMP with time zone not null default CURRENT_TIMESTAMP)`
	_, err = s.dbConnections.ExecContext(ctx, createTableSQL)
	if err != nil {
		logger.Log.Error("Create table failed", zap.Error(err))
		return nil, err
	}
	const createIndexUserStatusSQL = `CREATE index IF NOT EXISTS user_status_ix ON transactions (userid,status)`
	_, err = s.dbConnections.ExecContext(ctx, createIndexUserStatusSQL)
	if err != nil {
		logger.Log.Error("Create ix_id_orders failed", zap.Error(err))
		return nil, err
	}
	const createIndexNumberTypeSQL = `CREATE unique index IF NOT EXISTS number_type_uix ON transactions (number,type)`
	_, err = s.dbConnections.ExecContext(ctx, createIndexNumberTypeSQL)
	if err != nil {
		logger.Log.Error("Create ix_id_orders failed", zap.Error(err))
		return nil, err
	}
	const createIndexUserTypeSQL = `CREATE index IF NOT EXISTS user_type_ix ON transactions (userid,type)`
	_, err = s.dbConnections.ExecContext(ctx, createIndexUserTypeSQL)
	if err != nil {
		logger.Log.Error("Create ix_id_orders failed", zap.Error(err))
		return nil, err
	}
	return s, nil
}

func (ms *PGOrdersStorage) AddOrder(ctx context.Context, order *domain.Order) error {
	const insertSQL = `insert into transactions (userid,type,number,status,amount,uploaded_at) values ($1,'ORDER',$2,$3,$4,$5)`
	_, err := ms.dbConnections.ExecContext(ctx, insertSQL, order.UserID, order.Number, order.Status, order.Accrual, time.Time(order.UploadedAt))
	if err != nil {
		logger.Log.Error("Insert user failed", zap.Error(err))
		return fmt.Errorf("insert: %w", err)
	}
	return nil
}

func (ms *PGOrdersStorage) GetOrder(ctx context.Context, order *string) (*domain.Order, error) {
	const selectSQL = `select userid,number,status,amount,uploaded_at from transactions where number = $1 and type = 'ORDER'`
	row := ms.dbConnections.QueryRowContext(ctx, selectSQL, order)
	ret := domain.Order{}
	var uploadedAt time.Time
	err := row.Scan(&ret.UserID, &ret.Number, &ret.Status, &ret.Accrual, &uploadedAt)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		logger.Log.Error("Select user", zap.Error(err))
		return nil, fmt.Errorf("select: %w", err)
	}
	ret.UploadedAt = domain.CustomTime(uploadedAt)
	return &ret, nil
}

func (ms *PGOrdersStorage) GetAllOrders(ctx context.Context, UserID *int64) (*[]domain.Order, error) {
	const selectSQL = `select userid,number,status,amount,uploaded_at from transactions where userid = $1 and type = 'ORDER'`
	rows, err := ms.dbConnections.QueryContext(ctx, selectSQL, UserID)
	if err != nil {
		logger.Log.Error("Select all orders", zap.Error(err))
		return nil, fmt.Errorf("select all orders: %w", err)
	}
	if err = rows.Err(); err != nil {
		logger.Log.Error("Select all orders", zap.Error(err))
		return nil, fmt.Errorf("select all orders: %w", err)
	}
	defer rows.Close()
	order := domain.Order{}
	ret := make([]domain.Order, 0, 10)
	for rows.Next() {
		err = rows.Scan(&order.UserID, &order.Number, &order.Status, &order.Accrual, &order.UploadedAt)
		if err != nil {
			logger.Log.Error("Scan rows failed", zap.Error(err))
			return nil, err
		}
		if *order.Accrual == 0 {
			order.Accrual = nil
		}
		ret = append(ret, order)
	}
	if len(ret) == 0 {
		return nil, nil
	}
	return &ret, nil
}
func (ms *PGOrdersStorage) GetAllWithdraw(ctx context.Context, UserID *int64) (*[]domain.Withdraw, error) {
	const selectSQL = `select userid,number,-1*amount,uploaded_at from transactions where userid = $1 and type = 'WITHDRAW'`
	rows, err := ms.dbConnections.QueryContext(ctx, selectSQL, UserID)
	if err != nil {
		logger.Log.Error("Select all withdraws", zap.Error(err))
		return nil, fmt.Errorf("select all withdraws: %w", err)
	}
	if err = rows.Err(); err != nil {
		logger.Log.Error("Select all withdraws", zap.Error(err))
		return nil, fmt.Errorf("select all withdraws: %w", err)
	}
	defer rows.Close()
	withdraw := domain.Withdraw{}
	ret := make([]domain.Withdraw, 0, 10)
	for rows.Next() {
		err = rows.Scan(&withdraw.UserID, &withdraw.Order, &withdraw.Sum, &withdraw.ProcessedAt)
		if err != nil {
			logger.Log.Error("Scan rows failed", zap.Error(err))
			return nil, err
		}
		ret = append(ret, withdraw)
	}
	if len(ret) == 0 {
		return nil, nil
	}
	return &ret, nil
}
func (ms *PGOrdersStorage) GetBalance(ctx context.Context, UserID *int64) (*domain.Balance, error) {
	const selectSQL = `select COALESCE(sum(amount),0) current ,
       						  COALESCE(sum(case when type = 'WITHDRAW' then -1*amount else 0 end),0) withdrawn  
						from transactions 
						where userid = $1 and status = 'PROCESSED'`
	row := ms.dbConnections.QueryRowContext(ctx, selectSQL, UserID)
	ret := domain.Balance{UserID: *UserID, Current: 0, Withdrawn: 0}
	err := row.Scan(&ret.Current, &ret.Withdrawn)
	if err != nil {
		logger.Log.Error("Select balance", zap.Error(err))
		return nil, fmt.Errorf("select balance: %w", err)
	}
	return &ret, nil
}

func (ms *PGOrdersStorage) AddWithdraw(ctx context.Context, withdraw *domain.Withdraw) error {
	const insertSQL = `insert into transactions (userid,type,number,status,amount,uploaded_at) values ($1,'WITHDRAW',$2,'PROCESSED',-1*$3,$4)`
	_, err := ms.dbConnections.ExecContext(ctx, insertSQL, withdraw.UserID, withdraw.Order, withdraw.Sum, time.Time(withdraw.ProcessedAt))
	if err != nil {
		logger.Log.Error("Insert user failed", zap.Error(err))
		return fmt.Errorf("insert: %w", err)
	}
	return nil
}
func (ms *PGOrdersStorage) GetWithdraw(ctx context.Context, orderNumber *string) (*domain.Withdraw, error) {
	const selectSQL = `select userid,number,-1*amount,uploaded_at from transactions where number = $1 and type = 'WITHDRAW'`
	row := ms.dbConnections.QueryRowContext(ctx, selectSQL, orderNumber)
	ret := domain.Withdraw{}
	var processedAt time.Time
	err := row.Scan(&ret.UserID, &ret.Order, &ret.Sum, &processedAt)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		logger.Log.Error("Select user", zap.Error(err))
		return nil, fmt.Errorf("select: %w", err)
	}
	ret.ProcessedAt = domain.CustomTime(processedAt)
	return &ret, nil
}

func (ms *PGOrdersStorage) IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgerrcode.IsConnectionException(pgErr.Code)
}

func (ms *PGOrdersStorage) GetUnprocessedOrders(ctx context.Context, batchLimit *int) (*[]domain.Order, error) {
	const selectSQL = `select number from transactions where status in ('NEW','PROCESSING','REGISTERED') and type = 'ORDER' limit $1`
	rows, err := ms.dbConnections.QueryContext(ctx, selectSQL, batchLimit)
	if err != nil {
		logger.Log.Error("Select orders", zap.Error(err))
		return nil, fmt.Errorf("select orders: %w", err)
	}
	if err = rows.Err(); err != nil {
		logger.Log.Error("Select orders", zap.Error(err))
		return nil, fmt.Errorf("select orders: %w", err)
	}
	defer rows.Close()
	order := domain.Order{}
	ret := make([]domain.Order, 0, 10)
	for rows.Next() {
		err = rows.Scan(&order.Number)
		if err != nil {
			logger.Log.Error("Scan rows", zap.Error(err))
			return nil, fmt.Errorf("scan rows: %w", err)
		}
		ret = append(ret, order)
	}
	if len(ret) == 0 {
		return nil, nil
	}
	return &ret, nil
}

func (ms *PGOrdersStorage) SetProcessedAccruals(ctx context.Context, orders *[]domain.Accrual) error {
	tx, err := ms.dbConnections.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("open transaction: %w", err)
	}
	defer tx.Rollback()

	const updateSQL = `update transactions set status = $1 , amount = $2 where type = 'ORDER' and number = $3`
	stmt, err := tx.PrepareContext(ctx, updateSQL)
	if err != nil {
		return fmt.Errorf("prepare sql: %w", err)
	}
	defer stmt.Close()

	for _, v := range *orders {
		if v.Sum == nil {
			amount := domain.CustomMoney(0)
			v.Sum = &amount
		}
		_, err = stmt.ExecContext(ctx, v.Status, v.Sum, v.Order)
		if err != nil {
			return fmt.Errorf("exec sql: %w", err)
		}
	}

	return tx.Commit()
}
