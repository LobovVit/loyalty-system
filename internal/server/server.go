package server

import (
	"context"
	"net/http"

	"github.com/LobovVit/loyalty-system/internal/config"
	"github.com/LobovVit/loyalty-system/internal/domain/actions"
	"github.com/LobovVit/loyalty-system/pkg/logger"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	config             *config.Config
	userStorage        *actions.UserStorage
	transactionStorage *actions.TransactionRepo
}

func New(ctx context.Context, config *config.Config) (*Server, error) {
	users, err := actions.GetUserStorage(ctx, config)
	if err != nil {
		return nil, err
	}
	transactions, err := actions.GetTransactionRepo(ctx, config)
	if err != nil {
		return nil, err
	}
	return &Server{config: config, userStorage: &users, transactionStorage: &transactions}, nil
}

func (a *Server) Run(ctx context.Context) error {

	mux := chi.NewRouter()
	mux.Use(a.WithLogging)
	//mux.Use(a.WithCompress)

	mux.Post("/api/user/register", a.registerNewUser) //регистрация пользователя;
	mux.Post("/api/user/login", a.loginUser)          //аутентификация пользователя;
	mux.Route("/api/user", func(mux chi.Router) {
		mux.Use(a.Auth)
		mux.Post("/orders", a.loadOrders)              //загрузка пользователем номера заказа для расчёта;
		mux.Get("/orders", a.getOrders)                //получение списка загруженных пользователем номеров заказов, статусов их обработки и информации о начислениях;
		mux.Get("/balance", a.getBalance)              //получение текущего баланса счёта баллов лояльности пользователя;
		mux.Post("/balance/withdraw", a.debitingFunds) //запрос на списание баллов с накопительного счёта в счёт оплаты нового заказа;
		mux.Get("/withdrawals", a.debitHistory)        // получение информации о выводе средств с накопительного счёта пользователем.
	})

	logger.Log.Info("Starting server", zap.String("address", a.config.Host))

	httpServer := &http.Server{
		Addr:    a.config.Host,
		Handler: mux,
	}

	g := errgroup.Group{}
	g.Go(func() error {
		return httpServer.ListenAndServe()
	})
	g.Go(func() error {
		return a.transactionStorage.RunProcessing(ctx, a.config.BatchLimit, a.config.SendLimit, a.config.PollInterval)
	})
	g.Go(func() error {
		<-ctx.Done()
		return httpServer.Shutdown(ctx)
	})

	if err := g.Wait(); err != nil {
		logger.Log.Info("Shutdown", zap.Error(err))
		a.RouterShutdown(ctx)
	}
	return nil
}

func (a *Server) RouterShutdown(ctx context.Context) {
	logger.Log.Info("Router shutting down gracefully")
}
