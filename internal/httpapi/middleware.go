package httpapi

import (
	"crypto/subtle"
	"net/http"
)

// requireAPIKey checks the X-API-Key header against key using a
// constant-time comparison (avoids leaking key length/prefix via timing).
// If key is empty, auth is disabled — useful for local development.
func requireAPIKey(key string, next http.Handler) http.Handler {
	if key == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("X-API-Key")
		if subtle.ConstantTimeCompare([]byte(got), []byte(key)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}
