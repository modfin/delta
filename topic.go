package delta

import (
	"fmt"
	"strings"
)

func checkStreamName(stream string) (string, error) {
	stream = strings.TrimSpace(stream)
	stream = strings.ToLower(stream)
	stream = strings.ReplaceAll(stream, " ", "_")
	stream = strings.ReplaceAll(stream, "-", "_")
	stream = strings.ReplaceAll(stream, ".", "_")

	for _, r := range stream {
		if 'a' <= r && r <= 'z' {
			continue
		}
		if '0' <= r && r <= '9' {
			continue
		}
		switch r {
		case '_':
			continue
		}
		return "", fmt.Errorf("stream_ contains invalid character, only a-z, 0-9 and _ are allowed, name contains '%c'", r)
	}
	return stream, nil
}

func splitTopic(topic string) []string {
	var groups []string
	var currentGroup strings.Builder
	var depth int
	var inGroup bool

	// Extract groups by iterating through characters
	for _, char := range topic {
		if char == '{' {
			depth++
			if depth == 1 {
				inGroup = true
				continue // Skip the opening brace for the group content
			}
		}
		if inGroup && char == '}' {
			depth--
			if depth == 0 {
				groups = append(groups, currentGroup.String())
				currentGroup.Reset()
				inGroup = false
				continue // Skip the closing brace
			}
		}
		if inGroup {
			currentGroup.WriteRune(char)
		}
	}

	modifiedTopic := topic

	if inGroup { // Add the last unclosed group
		groups = append(groups, currentGroup.String())
		currentGroup.Reset()
		inGroup = false
		depth = 0
		modifiedTopic += "}"
	}

	// Replace groups with placeholders
	for _, group := range groups {
		modifiedTopic = strings.Replace(modifiedTopic, "{"+group+"}", "{}", 1)
	}

	// Split by dots
	parts := strings.Split(modifiedTopic, ".")

	// Replace placeholders with actual groups
	if len(groups) > 0 {
		groupIndex := 0
		for i := range parts {
			for strings.Contains(parts[i], "{}") && groupIndex < len(groups) {
				parts[i] = strings.Replace(parts[i], "{}", groups[groupIndex], 1)
				groupIndex++
			}
		}
	}

	return parts
}

// isAllowedTopicRune reports whether r is permitted in a plain (non-brace-group)
// topic segment. The allowlist is [a-z0-9_.*] where * is used for glob wildcards.
func isAllowedTopicRune(r rune) bool {
	return (r >= 'a' && r <= 'z') ||
		(r >= '0' && r <= '9') ||
		r == '_' || r == '.' || r == '*'
}

func checkTopic(topic string) (string, error) {
	topic = strings.TrimSpace(topic)
	topic = strings.Trim(topic, ".")
	topic = strings.ToLower(topic)
	if len(topic) == 0 {
		return "", fmt.Errorf("topic is empty")
	}

	parts := splitTopic(topic)

	for i := range parts {
		if strings.Contains(parts[i], ".") {
			// This part originated from a brace-enclosed group with dots (e.g.
			// "a.email.with.dots@example.com"). Treat it as an opaque literal key
			// and skip character validation; just re-wrap it in braces.
			parts[i] = "{" + parts[i] + "}"
			continue
		}
		// Plain segment: enforce the allowlist [a-z0-9_.*].
		for _, r := range parts[i] {
			if !isAllowedTopicRune(r) {
				return "", fmt.Errorf("topic contains invalid character %q; only [a-z0-9_.*] are allowed in plain segments", r)
			}
		}
	}
	return strings.Join(parts, "."), nil
}
