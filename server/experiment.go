package server

import (
	"strconv"
)

type Experiment struct {
	Id          uint64    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Hypothesis  string    `json:"hypothesis"`
	Variants    []Variant `json:"variants"`
}

func (exp *Experiment) Key() (key []byte) {
	return append([]byte("experiment:"), []byte(strconv.FormatUint(exp.Id, 10))...)
}

func (exp *Experiment) BucketUniqueKey(id uint64) (key []byte) {
	key = []byte("bucket-users:")
	key = append(key, []byte(strconv.FormatUint(exp.Id, 10))...)
	key = append(key, []byte(":")...)
	return append(key, []byte(strconv.FormatUint(id, 10))...)
}

func (exp *Experiment) BucketImpressionsKey(id uint64) (key []byte) {
	key = []byte("bucket-impressions:")
	key = append(key, []byte(strconv.FormatUint(exp.Id, 10))...)
	key = append(key, []byte(":")...)
	return append(key, []byte(strconv.FormatUint(id, 10))...)
}

func (exp *Experiment) ConvertUniqueKey(id uint64) (key []byte) {
	key = []byte("convert-users:")
	key = append(key, []byte(strconv.FormatUint(exp.Id, 10))...)
	key = append(key, []byte(":")...)
	return append(key, []byte(strconv.FormatUint(id, 10))...)
}

func (exp *Experiment) ConvertImpressionsKey(id uint64) (key []byte) {
	key = []byte("convert-impressions:")
	key = append(key, []byte(strconv.FormatUint(exp.Id, 10))...)
	key = append(key, []byte(":")...)
	return append(key, []byte(strconv.FormatUint(id, 10))...)
}

type Variant struct {
	Id      uint64 `json:"id"`
	Name    string `json:"name"`
	Value   string `json:"value"`
	Control bool   `json:"control"`
	Weight  int64  `json:"weight"`
}
