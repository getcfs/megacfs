package locmap

import "os"

var RUN_LONG bool = false

func init() {
	if os.Getenv("long_test") == "true" {
		RUN_LONG = true
	}
}
