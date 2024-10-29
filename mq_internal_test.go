package delta

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_checkTopicSub(t *testing.T) {
	type args struct {
		topic string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "empty",
			args:    args{topic: ""},
			want:    "",
			wantErr: assert.Error,
		},
		{
			name:    "a.B.c",
			args:    args{topic: "a.B.c"},
			want:    "a.b.c",
			wantErr: assert.NoError,
		},
		{
			name:    "a.b.c>",
			args:    args{topic: "a.b.c>"},
			want:    "",
			wantErr: assert.Error,
		},
		{
			name:    "a.b.c.*",
			args:    args{topic: "a.b.c.*"},
			want:    "a.b.c.*",
			wantErr: assert.NoError,
		},
		{
			name:    "a_b.**",
			args:    args{topic: "a_b.**"},
			want:    "a_b.**",
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := checkTopicSub(tt.args.topic)
			if !tt.wantErr(t, err, fmt.Sprintf("checkTopicSub(%v)", tt.args.topic)) {
				return
			}
			assert.Equalf(t, tt.want, got, "checkTopicSub(%v)", tt.args.topic)
		})
	}
}
