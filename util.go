package delta

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func URITemp() string {
	d := fmt.Sprintf("%d-delta", time.Now().UnixNano())
	uri := filepath.Join(os.TempDir(), d, "delta.db")
	os.MkdirAll(filepath.Dir(uri), 0755)
	return fmt.Sprintf("file:%s?tmp=true", uri)
}

func URIFromPath(path string) string {
	_ = os.MkdirAll(filepath.Dir(path), 0755)
	return fmt.Sprintf("file:%s", path)
}
