package metadata

import "testing"

func TestSchemes(t *testing.T) {
	cases := []struct {
		name        string
		s           Scheme
		total       int
		replication bool
	}{
		{"replication3", Replication3, 3, true},
		{"ec42", EC42, 6, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.s.Total(); got != c.total {
				t.Errorf("Total() = %d, want %d", got, c.total)
			}
			if got := c.s.IsReplication(); got != c.replication {
				t.Errorf("IsReplication() = %v, want %v", got, c.replication)
			}
		})
	}
}
