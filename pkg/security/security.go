package security

import (
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func CreateHash(password, salt string) string {
	var s = append([]byte(password), []byte(salt)...)
	return fmt.Sprintf("%x", sha256.Sum256(s))
}

func CheckHash(password, hash, salt string) bool {
	var s = append([]byte(password), []byte(salt)...)
	return hash == fmt.Sprintf("%x", sha256.Sum256(s))
}

type Claims struct {
	jwt.RegisteredClaims
	UserID int64
}

func BuildJWTString(userID int64, tokenExp time.Duration, jwtKey string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(tokenExp)),
		},
		UserID: userID,
	})

	tokenString, err := token.SignedString([]byte(jwtKey))
	if err != nil {
		return "", err
	}
	return tokenString, nil
}

func GetUserID(tokenString string, jwtKey string) (int64, error) {
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenString, claims,
		func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return []byte(jwtKey), nil
		})
	if err != nil {
		return -1, err
	}
	if !token.Valid {
		return -1, err
	}
	return claims.UserID, nil
}

func ValidLuhn(number int64) bool {
	return (number%10+checksum(number/10))%10 == 0
}

func checksum(number int64) int64 {
	var luhn int64
	for i := 0; number > 0; i++ {
		cur := number % 10
		if i%2 == 0 { // even
			cur = cur * 2
			if cur > 9 {
				cur = cur%10 + cur/10
			}
		}
		luhn += cur
		number = number / 10
	}
	return luhn % 10
}
