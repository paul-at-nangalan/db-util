package connect

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/paul-at-nangalan/errorhandler/handlers"
	"github.com/paul-at-nangalan/json-config/cfg"
	"os"
)

type PostgresCfg struct{
	Username string
	Password string
	Host string
	Database string
	CAFile string
	Sslmode string
	Port string

	basedir string
}

func (p *PostgresCfg) Expand() {
	p.Username = os.ExpandEnv(p.Username)
	p.Password = os.ExpandEnv(p.Password)
	p.Host = os.ExpandEnv(p.Host)
	p.Database = os.ExpandEnv(p.Database)
	p.CAFile = os.ExpandEnv(p.CAFile)
	p.Port = os.ExpandEnv(p.Port)
}

func Connect()*sql.DB {
	postgrescfg := PostgresCfg{}
	err := cfg.Read("postgres", &postgrescfg)
	handlers.PanicOnError(err)
	return connect(postgrescfg)
}
/// For testing
func connect(postgrescfg PostgresCfg)*sql.DB{
	constr := `host=` + postgrescfg.Host + ` ` +
		`dbname=`+ postgrescfg.Database + ` ` +
		`user=` + postgrescfg.Username + ` ` +
		`password=` + postgrescfg.Password + ` ` +
		`sslmode=` + postgrescfg.Sslmode + ` ` +
		`sslrootcert=` + postgrescfg.CAFile
	if postgrescfg.Port != ""{
		fmt.Println("port is ", postgrescfg.Port)
		//constr += " port=" + postgrescfg.Port
	}
	fmt.Println("Postgres user: ", postgrescfg.Username)
	db, err := sql.Open("postgres", constr)
	handlers.PanicOnError(err)

	return db
}
