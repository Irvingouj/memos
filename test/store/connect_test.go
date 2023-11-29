package teststore

import (
	"testing"

	"github.com/usememos/memos/store/db"
	"github.com/usememos/memos/test"
)

func TestConnection(t *testing.T) {
	profile := test.GetTestingProfile(t)
	_, err := db.NewDBDriver(profile)
	if err != nil {
		t.Log("Database connection failed", err)
		t.Fail()
	} else {
		t.Log("Database connection success")

	}
}
