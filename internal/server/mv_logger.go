package server

import (
	"net/http"
	"time"

	"go.uber.org/zap"
	"loyalty-system/pkg/logger"
)

type (
	responseData struct {
		status int
		size   int
	}
	loggingResponseWriter struct {
		http.ResponseWriter
		responseData *responseData
	}
)

func (r *loggingResponseWriter) WriteHeader(statusCode int) {
	r.ResponseWriter.WriteHeader(statusCode)
	r.responseData.status = statusCode
}

func (r *loggingResponseWriter) Write(b []byte) (int, error) {
	size, err := r.ResponseWriter.Write(b)
	r.responseData.size += size
	return size, err
}

func (a *Server) WithLogging(h http.Handler) http.Handler {
	logFn := func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		responseData := &responseData{
			status: 0,
			size:   0,
		}
		lw := loggingResponseWriter{
			ResponseWriter: w,
			responseData:   responseData,
		}
		h.ServeHTTP(&lw, r)

		duration := time.Since(start)

		defer func() {
			logger.Log.Info("Handler log",
				zap.String("uri", r.RequestURI),
				zap.String("method", r.Method),
				zap.Durationp("duration", &duration),
				zap.Int("status", responseData.status),
				zap.Int("size", responseData.size),
			)
		}()
	}
	return http.HandlerFunc(logFn)
}
