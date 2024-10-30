package delta

import (
	"fmt"
	"strings"
	"sync"
)

func globMatcher(topic, glob string) (bool, error) {
	globParts := strings.Split(glob, ".")
	topicParts := strings.Split(topic, ".")

	l := min(len(globParts), len(topicParts))
	for i := 0; i < l; i++ {
		if globParts[i] == "*" {
			continue
		}
		if globParts[i] == "**" {
			return true, nil
		}
		if globParts[i] != topicParts[i] {
			return false, nil
		}
	}
	return true, nil
}

type globbable interface {
	Id() string // uniquely identify the subscriber
	Topic() string
}

type globber[T globbable] struct {
	children map[string]*globber[T]

	pattern string
	subs    []T
	mu      sync.RWMutex
}

func newGlobber[T globbable]() *globber[T] {
	return &globber[T]{
		children: make(map[string]*globber[T]),
	}
}

func (t *globber[T]) Insert(sub T) {
	t.mu.Lock()
	defer t.mu.Unlock()
	parts := strings.Split(sub.Topic(), ".")
	node := t
	for _, part := range parts {

		if _, ok := node.children[part]; !ok {
			node.children[part] = newGlobber[T]()
		}
		node = node.children[part]
		node.pattern = part
		if part == "**" {
			break
		}
	}
	node.subs = append(node.subs, sub)
}

func (t *globber[T]) Remove(tbr T) {
	t.mu.Lock()
	defer t.mu.Unlock()
	parts := strings.Split(tbr.Topic(), ".")
	node := t
	var nodes []*globber[T]

	nodes = append(nodes, node)

	for _, part := range parts {
		if _, ok := node.children[part]; !ok {
			return
		}
		node = node.children[part]
		nodes = append(nodes, node)
		if part == "**" {
			break
		}
	}
	for i, sub := range node.subs {
		if sub.Id() == tbr.Id() {
			node.subs = append(node.subs[:i], node.subs[i+1:]...)
			// remove empty nodes
			for i := len(nodes) - 1; i > 0; i-- {
				if len(nodes[i].subs) > 0 {
					break
				}
				if len(nodes[i].children) > 0 {
					break
				}
				delete(nodes[i-1].children, nodes[i].pattern)
			}
		}

		return
	}
}

func (t *globber[T]) Print() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	t.printRecursive("")

}

func (t *globber[T]) Match(topic string) []T {
	t.mu.RLock()
	defer t.mu.RUnlock()
	parts := strings.Split(topic, ".")
	return t.matchRecursive(parts)
}

func (t *globber[T]) matchRecursive(topic []string) []T {

	if len(topic) == 0 {
		return t.subs
	}

	var matches []T
	node := t
	head := topic[0]

	if child, ok := node.children[head]; ok {
		matches = append(matches, child.matchRecursive(topic[1:])...)
	}
	if child, ok := node.children["*"]; ok {
		matches = append(matches, child.matchRecursive(topic[1:])...)
	}
	if child, ok := node.children["**"]; ok {
		matches = append(matches, child.subs...)

	}

	return matches
}

func (t *globber[T]) printRecursive(s string) {
	if len(t.subs) > 0 {
		fmt.Println(s, t.subs)
	}
	for k, v := range t.children {
		v.printRecursive(s + "." + k)
	}
}
