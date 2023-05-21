package connect

import (
	"fmt"
	"runtime/debug"
	"testing"
)

func checkPanic(t *testing.T){
	if r := recover(); r != nil{
		fmt.Println("ERROR: ", r)
		debug.PrintStack()
		t.Error("Failed to connect")
	}
}

func Test_Connection(t *testing.T){
//	defer checkPanic(t)
	pgcfg := PostgresCfg{
		Port: "$DBPORT",
		Host: "$DBHOST",
		Username: "$DBUSER",
		Password: "$DBPASSWD",
		Database: "$DBDATABASE",
		CAFile: "$CAFILE",
		Sslmode: "require",

	}
	pgcfg.Expand()
	db := connect(pgcfg)

	if db == nil{
		t.Error("Db is nil")
	}
	
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS test_connect_1234(param text)")
	if err != nil{
		t.Error("Failed to create table ", err.Error())
	}
}