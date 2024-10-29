package delta

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

type glob struct {
	id      string
	pattern string
}

func (g glob) Topic() string {
	return g.pattern
}

func (g glob) Id() string {
	return g.id
}

//func TestGlob(t *testing.T) {
//
//	trie := newGlobber[glob]()
//
//	gobs := []glob{
//		glob{"1", "a.b.c.d"},
//		glob{"1.5", "a.b.c.*"},
//		glob{"2", "a.*.c.d"},
//		glob{"2.5", "a.*.c"},
//		glob{"3", "a.b.**"},
//		glob{"4", "a.b.**"},
//		glob{"5", "a.b.c.*"},
//		glob{"6", "a.**"},
//		glob{"7", "a.b.c.d.e."},
//		glob{"8", "a.b.c.d.e.*"},
//	}
//
//	for _, g := range gobs {
//		trie.Insert(g)
//	}
//	//
//
//	matching := []string{
//		"a.b.c.d",     //  [{1 a.b.c.d} {1.5 a.b.c.*} {2 a.*.c.d} {3 a.b.**} {4 a.b.**} {5 a.b.c.*} {6 a.**}]
//		"a.b.c.d.e.f", //  [{3 a.b.**} {4 a.b.**} {6 a.**} {8 a.b.c.d.e.*}]
//		"a.x.c.d",     //  [{2 a.*.c.d} {6 a.**}]
//	}
//
//	for _, m := range matching {
//		s := trie.Match(m)
//		sort.Slice(s, func(i, j int) bool {
//			return s[i].Id() < s[j].Id()
//		})
//		fmt.Println("match", m, s)
//	}
//
//	fmt.Println("- removing", "a.*.c.d")
//	fmt.Println("match", "a.x.c.d", trie.Match("a.x.c.d")) //  [{6 a.**}]
//	fmt.Println("match", "a.x.c", trie.Match("a.x.c"))     // [{2.5 a.*.c} {6 a.**}]
//}

func TestGlobTopic_Insert(t *testing.T) {
	trie := newGlobber[glob]()
	trie.Insert(glob{id: "1", pattern: "a.b.c.d"})
	trie.Insert(glob{id: "2", pattern: "a.*.c.d"})
	trie.Insert(glob{id: "3", pattern: "a.b.**"})

	assert.Equal(t, 1, len(trie.children["a"].children["b"].children["c"].children["d"].subs))
	assert.Equal(t, 1, len(trie.children["a"].children["*"].children["c"].children["d"].subs))
	assert.Equal(t, 1, len(trie.children["a"].children["b"].children["**"].subs))
}

func TestGlobTopic_Match(t *testing.T) {
	trie := newGlobber[glob]()
	trie.Insert(glob{id: "1", pattern: "a.b.c.d"})
	trie.Insert(glob{id: "2", pattern: "a.*.c.d"})
	trie.Insert(glob{id: "3", pattern: "a.b.**"})

	matches := trie.Match("a.b.c.d")
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Id() < matches[j].Id()
	})
	assert.Equal(t, 3, len(matches))
	assert.Equal(t, "1", matches[0].Id())
	assert.Equal(t, "2", matches[1].Id())
	assert.Equal(t, "3", matches[2].Id())

	matches = trie.Match("a.x.c.d")
	assert.Equal(t, 1, len(matches))
	assert.Equal(t, "2", matches[0].Id())

	matches = trie.Match("a.b.c.d.e.f")
	assert.Equal(t, 1, len(matches))
	assert.Equal(t, "3", matches[0].Id())
}

func TestGlobTopic_Remove(t *testing.T) {
	trie := newGlobber[glob]()
	trie.Insert(glob{id: "1", pattern: "a.b.c.d"})
	trie.Insert(glob{id: "2", pattern: "a.*.c.d"})
	trie.Insert(glob{id: "3", pattern: "a.b.**"})

	trie.Remove(glob{"2", "a.*.c.d"})
	matches := trie.Match("a.x.c.d")
	assert.Equal(t, 0, len(matches))

	trie.Remove(glob{"1", "a.b.c.d"})
	matches = trie.Match("a.b.c.d")
	assert.Equal(t, 1, len(matches))
	assert.Equal(t, "3", matches[0].Id())
}

func TestGlobTopic_InsertAndMatch(t *testing.T) {
	trie := newGlobber[glob]()

	gobs := []glob{
		{"1", "a.b.c.d"},
		{"1.5", "a.b.c.*"},
		{"2", "a.*.c.d"},
		{"2.5", "a.*.c"},
		{"3", "a.b.**"},
		{"4", "a.b.**"},
		{"5", "a.b.c.*"},
		{"6", "a.**"},
		{"7", "a.b.c.d.e."},
		{"8", "a.b.c.d.e.*"},
	}

	for _, g := range gobs {
		trie.Insert(g)
	}

	matching := map[string][]string{
		"a.b.c.d":     {"1", "1.5", "2", "3", "4", "5", "6"},
		"a.b.c.d.e.f": {"3", "4", "6", "8"},
		"a.x.c.d":     {"2", "6"},
	}

	for topic, expectedIDs := range matching {
		matches := trie.Match(topic)
		assert.Equal(t, len(expectedIDs), len(matches), "Incorrect number of matches for topic %s", topic)

		matchedIDs := make([]string, len(matches))
		for i, m := range matches {
			matchedIDs[i] = m.Id()
		}

		assert.ElementsMatch(t, expectedIDs, matchedIDs, "Incorrect matches for topic %s", topic)
	}
}

func TestGlobTopic_Remove2(t *testing.T) {

	var emptyLeaf func(g *globber[glob], path []string) bool
	emptyLeaf = func(g *globber[glob], path []string) bool {
		var empty bool

		if len(g.subs) == 0 && len(g.children) == 0 {
			fmt.Println("empty leaf", path)
			return true
		}
		for k, v := range g.children {
			empty = empty || emptyLeaf(v, append(path, k))
		}
		return empty
	}

	trie := newGlobber[glob]()

	gobs := []glob{
		{"1", "a.b.c.d"},
		{"1.5", "a.b.c.*"},
		{"2", "a.*.c.d"},
		{"2.5", "a.*.c"},
		{"3", "a.b.**"},
		{"4", "a.b.**"},
		{"5", "a.b.c.*"},
		{"6", "a.**"},
		{"7", "a.b.c.d.e."},
		{"8", "a.b.c.d.e.*"},
	}

	for _, g := range gobs {
		trie.Insert(g)
	}

	assert.False(t, emptyLeaf(trie, []string{}))

	trie.Remove(
		glob{"2", "a.*.c.d"})
	assert.False(t, emptyLeaf(trie, []string{}))

	matches := trie.Match("a.x.c.d")
	assert.Equal(t, 1, len(matches), "Expected 1 match after removing 'a.*.c.d'")
	assert.Equal(t, "6", matches[0].Id(), "Incorrect match after removing 'a.*.c.d'")

	trie.Remove(glob{"1", "a.b.c.d"})
	assert.False(t, emptyLeaf(trie, []string{}))

	matches = trie.Match("a.b.c.d")
	assert.Equal(t, 5, len(matches), "Expected 5 matches after removing 'a.b.c.d'")
	matchedIDs := make([]string, len(matches))
	for i, m := range matches {
		matchedIDs[i] = m.Id()
	}
	assert.ElementsMatch(t, []string{"1.5", "3", "4", "5", "6"}, matchedIDs, "Incorrect matches after removing 'a.b.c.d'")

	trie.Remove(glob{"3", "a.b.**"})
	assert.False(t, emptyLeaf(trie, []string{}))

	trie.Remove(glob{"4", "a.b.**"})
	assert.False(t, emptyLeaf(trie, []string{}))

	matches = trie.Match("a.b.c.d.e.f")
	//fmt.Println("matches", matches)
	assert.Equal(t, 2, len(matches), "Expected 2 matches after removing 'a.b.**'")
	matchedIDs = make([]string, len(matches))
	for i, m := range matches {
		matchedIDs[i] = m.Id()
	}
	assert.ElementsMatch(t, []string{"6", "8"}, matchedIDs, "Incorrect matches after removing 'a.b.**'")
}
