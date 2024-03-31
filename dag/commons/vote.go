package commons

type Vote struct {
	Approved  bool
	Signature string
	Hash      uint64
	Round     Round
}
