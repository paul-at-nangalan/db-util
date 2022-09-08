package batch

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/paul-at-nangalan/db-util/connect"
	"github.com/paul-at-nangalan/db-util/migrator"
	"github.com/paul-at-nangalan/errorhandler/handlers"
	"github.com/paul-at-nangalan/json-config/cfg"
	"os"
	"testing"
)

type TestStuff struct{
	db *sql.DB
	tablename string
}
var teststuff TestStuff

func setup(){

	teststuff.db = connect.Connect()

	///create a test table
	cols := map[string]string{
		"index": "integer",
		"field1": "text",
		"field2": "text",
		"float1": "double precision",
		"float2": "double precision",
	}
	primes := []string{"index"}
	indx := []string{"field2"}
	mig := migrator.NewMigrator(teststuff.db, migrator.DBTYPE_POSTGRES)
	mig.Migrate("create-test-batch-writer", "test_batch_writer",
		cols, indx, primes)
	_, err := teststuff.db.Exec("DELETE FROM test_batch_writer")
	handlers.PanicOnError(err)

	teststuff.tablename = "test_batch_writer"
}

func getNextTestStr(teststr string, indx, length int)(int, string){
	if (indx % len(teststr) + length) >= len(teststr){
		return indx + length, teststr[(indx % len(teststr)): len(teststr)]
	}
	return indx + length, teststr[(indx % len(teststr)) : (indx % len(teststr)) + length]
}

type TestRowData struct{
	field1 string
	field2 string
	float1 float64
	float2 float64
}

func Test_Writer(t *testing.T){
	teststr := "qwertyuiop[]asdfghjkl;'zxcvbnm,.//.,mnbvcxz;lkjhgfdsa][poiuytrewq"
	teststrindx := 0
	testfloat := 1.02
	increment := 0.13

	fields := []string{"index", "field1", "field2", "float1", "float2"}
	batchsize := 17

	testdata := make([]TestRowData, 0)
	for i := 0; i < (batchsize * 5) + 7; i++{
		testrow := TestRowData{
		}
		teststrindx, testrow.field1 = getNextTestStr(teststr, teststrindx, 9 )
		teststrindx, testrow.field2 = getNextTestStr(teststr, teststrindx, 9 )
		testrow.field1 += fmt.Sprintf("%d", i)

		testrow.float1 = testfloat
		testfloat += increment
		testrow.float2 = testfloat
		testfloat += increment
		testdata = append(testdata, testrow)
	}
	onconflict := `ON CONFLICT (index) DO UPDATE SET field2=excluded.field2`

	batchwriter := NewWriter(teststuff.db, teststuff.tablename, fields, onconflict, batchsize)

	for i, row := range testdata{
		_, err := batchwriter.Exec(i, row.field1, row.field2, row.float1, row.float2)
		if err != nil{
			t.Error("Unexpected error ", err)
			t.FailNow()
		}
	}
	_, err := batchwriter.Flush()
	if err != nil{
		t.Error("Unexpected error ", err)
		t.FailNow()
	}

	////get the data back and check it
	res, err := teststuff.db.Query(`SELECT index, field1, field2, float1, float2 FROM ` + teststuff.tablename +
		` ORDER BY index`)
	handlers.PanicOnError(err)
	defer res.Close()

	for i, row := range testdata{
		if !res.Next(){
			t.Error("Not enough rows returned, only got ", i)
			t.FailNow()
		}
		index := 0
		field1 := ""
		field2 := ""
		float1 := 0.0
		float2 := 0.0
		err := res.Scan(&index, &field1, &field2, &float1, &float2)
		handlers.PanicOnError(err)

		if index != i{
			t.Error("Index mismatch ", i, " vs ", index)
		}
		if row.field1 != field1{
			t.Error("Mismatch field 1 ", row.field1, " vs ", field1, "@", i)
		}
		if row.field2 != field2{
			t.Error("Mismatch field 2 ", row.field2, " vs ", field2, "@", i)
		}
		if row.float1 != float1{
			t.Error("Mismatch float 1 ", row.float1, " vs ", float1, "@", i)
		}
		if row.float2 != float2{
			t.Error("Mismatch float 2 ", row.float2, " vs ", float2, "@", i)
		}
	}

}

func TestMain(m *testing.M){

	cfgdir := ""
	flag.StringVar(&cfgdir, "cfg", "../ut-cfg", "Config dir")
	flag.Parse()

	cfg.Setup(cfgdir)
	setup()

	os.Exit(m.Run())
}
