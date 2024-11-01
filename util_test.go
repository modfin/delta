package delta

import (
	"fmt"
	"github.com/stretchr/testify/assert"
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
