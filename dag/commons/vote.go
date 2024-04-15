package commons

type Vote struct {
	Approved  bool
	Signature string
	Hash      VHash
	Round     Round
}
