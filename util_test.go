package delta

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestUID(t *testing.T) {
	u := uid()
	assert.NotEmpty(t, u)
	assert.Len(t, u, 29)

}

func BenchmarkUID(b *testing.B) {

	for i := 0; i < b.N; i++ {
		uid()
	}

	fmt.Println("done", b.N, uid())

}

func TestSplitTopic(t *testing.T) {
	testCases := []struct {
		input    string
		expected []string
	}{
		{"a.{b}", []string{"a", "b"}},
		{"a.{}.b", []string{"a", "", "b"}},
		{"a.b.c{d}", []string{"a", "b", "cd"}},
		{"x.{y}.z.{w}", []string{"x", "y", "z", "w"}},
		{"plain.topic", []string{"plain", "topic"}},
		{"{group.with.dots}.at.start", []string{"group.with.dots", "at", "start"}},
		{"end.with.{group.and.dots}", []string{"end", "with", "group.and.dots"}},
		{"end.wi}th.group", []string{"end", "wi}th", "group"}},
		{"{end}.wi}th.group", []string{"end", "wi}th", "group"}},
		{"end.wi}th.{group}", []string{"end", "wi}th", "group"}},
		{"end.with.{group.end", []string{"end", "with", "group.end"}},
		// This does not work as expected...
		// Probably should replace groups using regex with counting braces...
		{"no.{sub.groups.{are}.allowed}.here", []string{"no", "sub.groups.{are}.allowed", "here"}},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := splitTopic(tc.input)
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("splitTopic(%q) = %v, want %v", tc.input, result, tc.expected)
			}
		})
	}
}

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
