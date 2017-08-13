package hash

import (
	"errors"
	"math"
	"unicode/utf8"
)

var ErrInvalidHash = errors.New("invalid hash")

func NewEncoder(charStr string) Encoder {
	encoder := new(baseEncoder)
	encoder.chars, encoder.charsMap = uniqueChars([]rune(charStr))
	encoder.metric = int64(len(encoder.chars))
	return *encoder
}

// baseEncoder encodes the id using the mapping of the given charset.
type baseEncoder struct {
	chars    []rune
	charsMap map[rune]int
	metric   int64
}

func (e baseEncoder) Encode(id int64) (string, error) {
	encoded := make([]rune, 0, 16)
	for id >= e.metric {
		encoded = append(encoded, e.chars[id%e.metric])
		id = id / e.metric
	}
	encoded = append(encoded, e.chars[id])
	reverseChars(encoded)
	return string(encoded), nil
}

func (e baseEncoder) Decode(hash string) (id int64, err error) {
	cnt := utf8.RuneCountInString(hash)
	for k, v := range []rune(hash) {
		index, exist := e.charsMap[v]
		if !exist {
			return 0, ErrInvalidHash
		}
		id += int64(index * int(math.Pow(float64(len(e.chars)), float64(cnt-k-1))))
	}
	return id, nil
}

func uniqueChars(chars []rune) ([]rune, map[rune]int) {
	uniqMap := make(map[rune]int, len(chars))
	uniq := make([]rune, 0, len(chars))
	for _, char := range chars {
		if _, exist := uniqMap[char]; !exist {
			uniqMap[char] = len(uniqMap)
			uniq = append(uniq, char)
		}
	}
	return uniq[:len(uniqMap)], uniqMap
}

func reverseChars(chars []rune) {
	length := len(chars)
	bound := length / 2
	for i := 0; i < bound; i++ {
		chars[i], chars[length-i-1] = chars[length-i-1], chars[i]
	}
}
