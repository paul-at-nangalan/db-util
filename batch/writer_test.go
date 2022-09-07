package batch

import (
	"database/sql"
	"github.com/paul-at-nangalan/db-util/connect"
	"github.com/paul-at-nangalan/db-util/migrator"
	"github.com/paul-at-nangalan/errorhandler/handlers"
	"testing"
)

type TestStuff struct{
	db *sql.DB
}
var teststuff TestStuff

func setup(){

	teststuff.db = connect.Connect()

	///create a test table
	cols := map[string]string{
		"field1": "text",
		"field2": "text",
		"float1": "double precision",
		"float2": "double precision",
	}
	primes := []string{"field1"}
	indx := []string{"field2"}
	mig := migrator.NewMigrator(teststuff.db, migrator.DBTYPE_POSTGRES)
	mig.Migrate("create-test-batch-writer", "test_batch_writer",
		cols, indx, primes)
	_, err := teststuff.db.Exec("DELETE FROM test_batch_writer")
	handlers.PanicOnError(err)
}

func Test_Writer(t *testing.T){

}
