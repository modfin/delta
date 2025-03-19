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
			want:    "a.b.c>",
			wantErr: assert.NoError,
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
		{
			name:    "a_b.{c}",
			args:    args{topic: "a_b.{c}"},
			want:    "a_b.c",
			wantErr: assert.NoError,
		},
		{
			name:    "a_b.{c}.d",
			args:    args{topic: "a_b.{c}.d"},
			want:    "a_b.c.d",
			wantErr: assert.NoError,
		},
		{
			name:    "a_b.{c.d}",
			args:    args{topic: "a_b.{c.d}"},
			want:    "a_b.{c.d}",
			wantErr: assert.NoError,
		},
		{
			name:    "a_b.{c.d}.e",
			args:    args{topic: "a_b.{c.d}.e"},
			want:    "a_b.{c.d}.e",
			wantErr: assert.NoError,
		},
		{
			name:    "a_b.{c.d}.{e}.f",
			args:    args{topic: "a_b.{c.d}.{e}.f"},
			want:    "a_b.{c.d}.e.f",
			wantErr: assert.NoError,
		},
		{
			name:    "a_b.{c.d}.{e}.{f}",
			args:    args{topic: "a_b.{c.d}.{e}.{f}"},
			want:    "a_b.{c.d}.e.f",
			wantErr: assert.NoError,
		},
		{
			name:    "{a_b}.{c.d}.{e}.{f}",
			args:    args{topic: "{a_b}.{c.d}.{e}.{f}"},
			want:    "a_b.{c.d}.e.f",
			wantErr: assert.NoError,
		},
		{
			name:    "{a_b.c.d.{e}.f}",
			args:    args{topic: "{a_b.c.d.{e}.f}"},
			want:    "{a_b.c.d.{e}.f}",
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := checkTopic(tt.args.topic)
			if !tt.wantErr(t, err, fmt.Sprintf("checkTopic(%v)", tt.args.topic)) {
				return
			}
			assert.Equalf(t, tt.want, got, "checkTopic(%v)", tt.args.topic)
		})
	}
}
