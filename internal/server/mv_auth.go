package server

import (
	"net/http"
	"strconv"

	"loyalty-system/pkg/security"
)

func (a *Server) Auth(h http.Handler) http.Handler {
	logFn := func(w http.ResponseWriter, r *http.Request) {
		ow := w
		jwt := r.Header.Get("Authorization")
		userID, err := security.GetUserID(jwt, a.config.JWTKey)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		user := strconv.FormatInt(userID, 10)
		r.Header.Add("user-id", user)
		h.ServeHTTP(ow, r)
	}
	return http.HandlerFunc(logFn)
}
