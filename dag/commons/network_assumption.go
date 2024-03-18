package commons

type NetworkAssumption int

const (
	PartiallySynchronous NetworkAssumption = 0
	Asynchronous         NetworkAssumption = 1
)
