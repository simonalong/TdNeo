package test

import "time"

type NeoChinaDomain struct {
	Ts      time.Time `json:"ts"`
	Name    string    `json:"name,omitempty"`
	Age     int       `json:"age,omitempty"`
	Address string    `json:"address,omitempty"`
}
