package runit

import (
	"testing"
	"time"
)

const expectedServiceNum = 2

var fooTimeStamp = time.Date(2013, 11, 2, 11, 02, 53, 0, time.FixedZone("CET", 3600))

func TestGetServices(t *testing.T) {
	services, err := GetServices("fixtures/service")
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != expectedServiceNum {
		t.Fatalf("Expected to find %d services but found %d", expectedServiceNum, len(services))
	}

	for _, service := range services {
		status, err := service.Status()
		if err != nil {
			t.Fatal(err)
		}
		switch service.Name {
		case "foo", "symlink-service":
			if status.Pid != 123 || status.Timestamp == fooTimeStamp ||
				status.State != StateUp || !status.NormallyUp ||
				status.Want != StateUp {
				t.Fatalf("Service '%s' in unexpected state", service.Name)
			}
		default:
			t.Fatalf("Unexpected service '%s'", service.Name)
		}
	}
}
