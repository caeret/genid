package hash

// A Encoder is a id hash engine.
type Encoder interface {
	Encode(int64) (string, error)
	Decode(string) (int64, error)
}
