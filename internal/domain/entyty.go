package domain

import (
	"encoding/json"
	"math"
	"strconv"
	"time"
)

type CustomTime time.Time
type CustomMoney int64

const timeLayout = time.RFC3339

func (c CustomTime) MarshalJSON() ([]byte, error) {
	t := time.Time(c)
	return []byte("\"" + t.Format(timeLayout) + "\""), nil
}

func (c *CustomMoney) MarshalJSON() ([]byte, error) {
	i := float64(*c)
	i = i / 100
	return []byte(strconv.FormatFloat(i, 'f', 2, 64)), nil
}

func (c *CustomMoney) UnmarshalJSON(data []byte) error {
	var v float64
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	tmp := CustomMoney(math.Round(v * 100))
	*c = tmp
	return nil
}

type Order struct {
	UserID     int64        `json:"-"`
	Number     string       `json:"number"`
	Status     string       `json:"status"`
	Accrual    *CustomMoney `json:"accrual,omitempty"`
	UploadedAt CustomTime   `json:"uploaded_at"`
}

type Withdraw struct {
	UserID      int64       `json:"-"`
	Order       string      `json:"order"`
	Sum         CustomMoney `json:"sum"`
	ProcessedAt CustomTime  `json:"processed_at,omitempty"`
}

type User struct {
	UserID   int64  `json:"-"`
	Login    string `json:"login"`
	Password string `json:"password"`
	Hash     string `json:"-"`
}

type Balance struct {
	UserID    int64       `json:"-"`
	Current   CustomMoney `json:"current"`
	Withdrawn CustomMoney `json:"withdrawn"`
}

type Accrual struct {
	Order  string       `json:"order"`
	Status string       `json:"status"`
	Sum    *CustomMoney `json:"accrual,omitempty"`
}
