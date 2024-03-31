package commons

type FabftBlock struct {
	Mkroot        string
	Sender        *NodeInfo
	Round         Round
	Transaction   string
	LastRoungTips []string
}

func (c *FabftBlock) Hash() string {
	return c.Mkroot
}
