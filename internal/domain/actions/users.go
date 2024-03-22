package actions

import (
	"context"
	"fmt"

	"github.com/LobovVit/loyalty-system/internal/config"
	"github.com/LobovVit/loyalty-system/internal/domain"
	"github.com/LobovVit/loyalty-system/internal/domain/dbstorage/pgusers"
	"github.com/LobovVit/loyalty-system/pkg/retry"
	"github.com/LobovVit/loyalty-system/pkg/security"
)

type UserExistsErr string
type WrongPasswordErr string
type UserNotExistsErr string

func (e UserExistsErr) Error() string    { return string(e) }
func (e WrongPasswordErr) Error() string { return string(e) }
func (e UserNotExistsErr) Error() string { return string(e) }

const UserExists UserExistsErr = "user already exists"
const WrongPassword WrongPasswordErr = "wrong password"
const UserNotExists UserNotExistsErr = "no such user"

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
		return UserExists
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
		return -1, UserNotExists
	}
	if !security.CheckHash(password, user.Hash, salt) {
		return -1, WrongPassword
	}
	return user.UserID, nil
}
