package connect

import (
	"database/sql"
	"github.com/paul-at-nangalan/json-config/cfg"
	"github.com/paul-at-nangalan/errorhandler/handlers"
	"os"
)

type PostgresCfg struct{
	Username string
	Password string
	Host string
	Database string
	CAFile string
	Sslmode string

	basedir string
}

func (p *PostgresCfg) Expand() {
	p.Username = os.ExpandEnv(p.Username)
	p.Password = os.ExpandEnv(p.Password)
	p.Host = os.ExpandEnv(p.Host)
	p.Database = os.ExpandEnv(p.Database)
	p.CAFile = os.ExpandEnv(p.CAFile)
}

func Connect()*sql.DB{
	postgrescfg := PostgresCfg{}
	err := cfg.Read("postgres", &postgrescfg)
	handlers.PanicOnError(err)
	constr := `host=` + postgrescfg.Host + ` ` +
		`dbname=`+ postgrescfg.Database + ` ` +
		`user=` + postgrescfg.Username + ` ` +
		`password=` + postgrescfg.Password + ` ` +
		`sslmode=` + postgrescfg.Sslmode + ` ` +
		`sslrootcert=` + postgrescfg.CAFile
	//fmt.Println("Postgres params: ", constr)
	db, err := sql.Open("postgres", constr)
	handlers.PanicOnError(err)

	return db
}
