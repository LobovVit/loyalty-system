package actions

import (
	"context"
	"errors"
	"fmt"

	"loyalty-system/internal/config"
	"loyalty-system/internal/domain"
	"loyalty-system/internal/domain/dbstorage/pgusers"
	"loyalty-system/pkg/retry"
	"loyalty-system/pkg/security"
)

var ErrUserExists = errors.New("user already exists")
var ErrWrongPassword = errors.New("wrong password")
var ErrUserNotExists = errors.New("no such user")

type UserStorage struct {
	userStorage users
}

type users interface {
	AddUser(ctx context.Context, user *domain.User) error
	GetUser(ctx context.Context, login *string) (*domain.User, error)
	IsRetryable(err error) bool
}

func GetUserStorage(ctx context.Context, config *config.Config) (UserStorage, error) {
	storage, err := pgusers.NewUserStorage(ctx, config.DSN)
	if err != nil {
		return UserStorage{}, fmt.Errorf("get user storage: %w", err)
	}
	return UserStorage{userStorage: storage}, nil
}

func (u *UserStorage) NewUser(ctx context.Context, login string, password string, salt string) error {
	user, err := retry.DoWithReturn(ctx, 3, u.userStorage.GetUser, &login, u.userStorage.IsRetryable)
	if err != nil {
		return fmt.Errorf("get user: %w", err)
	}
	if user != nil {
		return ErrUserExists
	}
	user = &domain.User{}
	user.Login = login
	user.Hash = security.CreateHash(password, salt)
	err = retry.DoWithoutReturn(ctx, 3, u.userStorage.AddUser, user, u.userStorage.IsRetryable)
	if err != nil {
		return fmt.Errorf("new user: %w", err)
	}
	return nil
}

func (u *UserStorage) LoginUser(ctx context.Context, login string, password string, salt string) (int64, error) {
	user, err := retry.DoWithReturn(ctx, 3, u.userStorage.GetUser, &login, u.userStorage.IsRetryable)
	if err != nil {
		return -1, fmt.Errorf("login user: %w", err)
	}
	if user == nil {
		return -1, ErrUserNotExists
	}
	if !security.CheckHash(password, user.Hash, salt) {
		return -1, ErrWrongPassword
	}
	return user.UserID, nil
}
