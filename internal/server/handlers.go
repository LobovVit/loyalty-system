package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"loyalty-system/internal/domain"
	"loyalty-system/internal/domain/actions"
	"loyalty-system/pkg/security"
)

func (a *Server) registerNewUser(w http.ResponseWriter, r *http.Request) {
	var user domain.User
	var buf bytes.Buffer
	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = json.Unmarshal(buf.Bytes(), &user); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if user.Login == "" || user.Password == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	err = a.userStorage.NewUser(r.Context(), user.Login, user.Password, a.config.Salt)
	if err != nil && errors.Is(err, actions.ErrUserExists) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	//
	userID, err := a.userStorage.LoginUser(r.Context(), user.Login, user.Password, a.config.Salt)
	if err != nil {
		switch {
		case errors.Is(err, actions.ErrWrongPassword):
			http.Error(w, err.Error(), http.StatusUnauthorized)
		case errors.Is(err, actions.ErrUserNotExists):
			http.Error(w, err.Error(), http.StatusUnauthorized)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	t, err := security.BuildJWTString(userID, time.Hour*time.Duration(a.config.JWTExp), a.config.JWTKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Add("Authorization", t)
	w.WriteHeader(http.StatusOK)
}

func (a *Server) loginUser(w http.ResponseWriter, r *http.Request) {
	var user domain.User
	var buf bytes.Buffer
	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = json.Unmarshal(buf.Bytes(), &user); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if user.Login == "" || user.Password == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	userID, err := a.userStorage.LoginUser(r.Context(), user.Login, user.Password, a.config.Salt)
	if err != nil {
		switch {
		case errors.Is(err, actions.ErrWrongPassword):
			http.Error(w, err.Error(), http.StatusUnauthorized)
		case errors.Is(err, actions.ErrUserNotExists):
			http.Error(w, err.Error(), http.StatusUnauthorized)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	t, err := security.BuildJWTString(userID, time.Hour*time.Duration(a.config.JWTExp), a.config.JWTKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Add("Authorization", t)
	w.WriteHeader(http.StatusOK)
}

func (a *Server) loadOrders(w http.ResponseWriter, r *http.Request) {
	userID, err := strconv.ParseInt(r.Header.Get("user-id"), 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
	}
	//
	var buf bytes.Buffer
	_, err = buf.ReadFrom(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	orderNum := buf.String()
	err = a.transactionStorage.NewOrder(r.Context(), userID, orderNum)
	w.Header().Set("Content-Type", "text/plain")
	switch {
	case errors.Is(err, actions.ErrOrderAccepted):
		w.WriteHeader(http.StatusAccepted)
	case errors.Is(err, actions.ErrOrderUploadedCurrUser):
		w.WriteHeader(http.StatusOK)
	case errors.Is(err, actions.ErrOrderFormat):
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
	case errors.Is(err, actions.ErrOrderUploadedAnotherUser):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, actions.ErrUnexpectedReturn):
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		http.Error(w, actions.ErrUnexpectedReturn.Error(), http.StatusInternalServerError)
	}

}

func (a *Server) getOrders(w http.ResponseWriter, r *http.Request) {
	userID, err := strconv.ParseInt(r.Header.Get("user-id"), 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
	}
	//
	orders, err := a.transactionStorage.GetAllOrders(r.Context(), &userID)
	if err != nil {
		switch {
		case errors.Is(err, actions.ErrNotExists):
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
	resp, err := json.MarshalIndent(orders, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(resp)
}

func (a *Server) getBalance(w http.ResponseWriter, r *http.Request) {
	userID, err := strconv.ParseInt(r.Header.Get("user-id"), 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
	}
	//
	balance, err := a.transactionStorage.GetBalance(r.Context(), &userID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	resp, err := json.MarshalIndent(balance, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(resp)
}

func (a *Server) debitingFunds(w http.ResponseWriter, r *http.Request) {
	userID, err := strconv.ParseInt(r.Header.Get("user-id"), 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
	}
	//
	withdraw := domain.Withdraw{}
	var buf bytes.Buffer
	_, err = buf.ReadFrom(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = json.Unmarshal(buf.Bytes(), &withdraw); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	withdraw.UserID = userID
	err = a.transactionStorage.NewWithdraw(r.Context(), withdraw)
	if err != nil {
		switch {
		case errors.Is(err, actions.ErrInsufficientFounds):
			http.Error(w, err.Error(), http.StatusPaymentRequired)
		case errors.Is(err, actions.ErrOrderUploadedCurrUser):
			w.WriteHeader(http.StatusOK)
		case errors.Is(err, actions.ErrOrderFormat):
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		case errors.Is(err, actions.ErrOrderUploadedAnotherUser):
			http.Error(w, err.Error(), http.StatusConflict)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (a *Server) debitHistory(w http.ResponseWriter, r *http.Request) {
	userID, err := strconv.ParseInt(r.Header.Get("user-id"), 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
	}
	//
	withdraws, err := a.transactionStorage.GetAllWithdraw(r.Context(), &userID)
	if err != nil {
		switch {
		case errors.Is(err, actions.ErrNotExists):
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
	resp, err := json.MarshalIndent(withdraws, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(resp)
}
