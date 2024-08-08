package adapters

import (
	"testing"

	"github.com/kndndrj/nvim-dbee/dbee/core/builders"
	"github.com/stretchr/testify/require"
)

func TestNewAthena(t *testing.T) {
	r := require.New(t)

	type args struct {
		rawURL string
	}

	tests := []struct {
		want    *athenaDriver
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Athena",
			args: args{
				rawURL: "athena://eu-central-1?work_group=nxAthena-v1",
			},
			want: &athenaDriver{
				c: &builders.Client{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := new(Athena).Connect(tt.args.rawURL)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewAthena() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				r.Nil(got)
				r.Error(err)
				return
			}
			r.NoError(err)
		})
	}
}
