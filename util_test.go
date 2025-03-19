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

	//for i := 0; i < 1000; i++ {
	//	fmt.Println(uid())
	//}

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
