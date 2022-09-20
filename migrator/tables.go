package migrator

import (
	"database/sql"
	"github.com/paul-at-nangalan/errorhandler/handlers"
	"log"
	"strings"
)

type DbType string

const(
	DBTYPE_MYSQL DbType = "mysql"
	DBTYPE_POSTGRES DbType = "postgres" ////NOTE: this has not been thoroughly used/tested
)

type Migrator struct{
	db *sql.DB
	migrationsmap map[string]bool

	dbtype DbType
}

func NewMigrator(db *sql.DB, dbtype DbType)*Migrator{
	createmigtable := `CREATE TABLE IF NOT EXISTS migrations (migration text)`
	_, err := db.Exec(createmigtable)
	handlers.PanicOnError(err)

	migrator := &Migrator{
		db: db,
		dbtype: dbtype,
	}
	migrator.loadMigs()
	return migrator
}

func (p *Migrator)loadMigs(){
	loadmigs := `SELECT migration FROM migrations`
	res, err := p.db.Query(loadmigs)
	handlers.PanicOnError(err)
	defer res.Close()

	migrations := make(map[string]bool)
	for res.Next(){
		mig := ""
		err := res.Scan(&mig)
		handlers.PanicOnError(err)
		migrations[mig] = true
	}
	p.migrationsmap = migrations
}


//columns is a map of column name to specification
/// Use Migrate to create a table
func (p *Migrator)createTable(
	name string, columns map[string]string, indexes []string, primarykeys []string){

	stmt := "Create table If not Exists " + name + " ("
	sep := ""
	for colname, coltype := range columns {
		stmt += sep + colname + " " + coltype
		sep = ", "
	}
	if p.dbtype == DBTYPE_MYSQL {
		for _, indexname := range indexes {
			stmt += sep + "index(" + indexname + ")"
			sep = ", "
		}
	}
	stmt += sep + " Primary Key(";
	sep = ""
	for _, primarykey := range primarykeys {
		stmt += sep +  primarykey
		sep = ", "
	}
	stmt += "))"
	log.Println("Create table stmt: " + stmt)
	_, err := p.db.Exec(stmt)
	if err != nil {
		log.Println("Failed to create " + name  + " table with error " + err.Error())
		log.Panic("Failed to create table")
	}
	if p.dbtype != DBTYPE_MYSQL{
		for _, index := range indexes{
			p.CreateIndex("add-index-" + name + "-" + index,
				             name, index)
		}
	}
}

func (p *Migrator)CreateIndex(migration string, table string, colname string){
	if _, ok := p.migrationsmap[migration]; ok {
		return
	}
	stmt := `CREATE INDEX ON ` + table + ` (` + colname + `)`
	_, err := p.db.Exec(stmt)
	if err != nil {
		log.Println("Failed to create index " + migration  + " with error " + err.Error())
		log.Panic("Failed to create index")
	}
	p.markMigration(migration)
}

func (p *Migrator)AlterTableAdd(migration string, name string, columns map[string]string){
	if _, ok := p.migrationsmap[migration]; ok {
		return
	}
	stmt := "Alter table " + name + " add "
	if p.dbtype == DBTYPE_MYSQL{
		stmt += "("
	}else if p.dbtype == DBTYPE_POSTGRES{
		stmt += " column "
	}
	sep := ""
	for colname, coltype := range columns {
		stmt += sep + colname + " " + coltype
		sep = ", "
	}
	if p.dbtype == DBTYPE_MYSQL {
		stmt += ")"
	}
	_, err := p.db.Exec(stmt)
	if err != nil {
		log.Println("Failed to create " + name  + " table with error " + err.Error())
		log.Panic("Failed to create table")
	}
	p.markMigration(migration)
}

func (p *Migrator)Migrate(migration string, tablename string, columns map[string]string, indexes []string, primarykeys []string){
	if _, ok := p.migrationsmap[migration]; !ok {
		///this migration is new
		p.createTable(tablename, columns, indexes, primarykeys)
		p.markMigration(migration)
	}
}

func (p *Migrator)markMigration(migration string){
	_,err := p.db.Exec("Insert into migrations (migration) VALUES($1)", migration)
	if err != nil {
		log.Println("Failed to set migration " + migration  + " table with error " + err.Error())
		log.Panic("Failed to create table")
	}
	log.Println("Migrated " + migration)
}

func (p *Migrator)Alter(migration string, tablename string, columns map[string]string){
	if _, ok := p.migrationsmap[migration]; !ok {
		///this migration is new
		p.AlterTableAdd(migration, tablename, columns)
	}
}

func (p *Migrator)AlterColumnDef(migration string, tablename string, columns map[string]string){
	if _, ok := p.migrationsmap[migration]; !ok {
		///this migration is new
		for colname, coltype := range columns {
			stmt := "Alter table " + tablename + " modify column "
			stmt +=  colname + " " + coltype
			_, err := p.db.Exec(stmt)
			if err != nil {
				log.Println("Failed to alter " + tablename  + " table with error " + err.Error())
				log.Panic("Failed to create table")
			}
		}

		p.markMigration(migration)
	}
}


func (p *Migrator)AlterPrimes(migration string, tablename string, primarykeys []string){
	if _, ok := p.migrationsmap[migration]; !ok {
		sep := ""
		allkeys := ""
		for _, key := range primarykeys {
			allkeys += sep + key
			sep = ", "
		}
		if p.dbtype == DBTYPE_MYSQL{
			_, err := p.db.Exec("alter table " + tablename + " drop primary key, add primary key(" + allkeys + ")")
			if err != nil {
				log.Println("Failed to alter primary keys ", err)
				log.Panic("Failed to alter primary keys")
			}
		}else{
			_, err := p.db.Exec("alter table " + tablename + " DROP CONSTRAINT " +
				tablename + "_pkey")
			if err != nil{
				log.Panicln("Failed to alter primary key ", err)
			}
			_, err = p.db.Exec("alter table " + tablename + " ADD PRIMARY KEY (" +
				allkeys + ")")
			if err != nil{
				log.Panicln("Failed to alter primary key ", err)
			}
		}

		p.markMigration(migration)
	}
}

func (p *Migrator)DropPrimes(migration string, tablename string){
	if _, ok := p.migrationsmap[migration]; !ok {
		_, err := p.db.Exec("alter table " + tablename + " drop primary key")
		if err != nil {
			log.Println("Failed to drop primary keys ", err)
			log.Panic("Failed to drop primary keys")
		}
		p.markMigration(migration)
	}
}

func (p *Migrator)AlterIndexes(migration string, tablename string, indexes []string){
	if _, ok := p.migrationsmap[migration]; !ok {
		sep := ""
		allindexes := ""
		for _, key := range indexes {
			allindexes += sep + "ADD INDEX(" + key + ")"
			sep = ", "
		}
		_, err := p.db.Exec("alter table " + tablename + " " + allindexes)
		if err != nil {
			log.Println("Failed to alter primary keys ", err)
			log.Panic("Failed to alter primary keys")
		}
		p.markMigration(migration)
	}
}


func (p *Migrator)MigrateRaw(migration string, qry string){
	if _, ok := p.migrationsmap[migration]; !ok {
		_, err := p.db.Exec(qry)
		if err != nil {
			log.Panic("Failed to run migration raw ", err, " in query ", qry)
		}
		p.markMigration(migration)
	}
}

func (p *Migrator)AddAuditTrigger(
	migration string, origtable string, cols map[string]string, createaudittable bool,
	audittable string){

	///older code may have differently named the audittable
	if audittable == "" {
		///this should be the standard
		audittable = "Audit" + origtable
	}
	triggername := "audit_" + strings.ToLower(origtable)

	if createaudittable {///if audit table exists ... don't recreate ... you'll need to alter it explicitly if required
		qry := `CREATE TABLE ` + audittable + ` LIKE ` + origtable;
		p.MigrateRaw(migration+"create_audit_table", qry)

		qry = `ALTER TABLE ` + audittable + ` ADD COLUMN DateOfChange TIMESTAMP DEFAULT NOW()`;
		p.MigrateRaw(migration+"alter_audit_table", qry)

		qry = `ALTER TABLE ` + audittable + ` DROP PRIMARY KEY`;
		p.MigrateRaw(migration+"drop_primes", qry)

		qry = `ALTER TABLE ` + audittable + ` ADD COLUMN idindx BIGINT AUTO_INCREMENT PRIMARY KEY`;
		p.MigrateRaw(migration+"add_idindx", qry)
	}
	qry := `CREATE TRIGGER ` + triggername + ` BEFORE UPDATE ON ` + origtable +
		` FOR EACH ROW
    			INSERT INTO ` + audittable + ` SET `
	sep := ""
	for colname, _ := range cols{
		qry += sep + colname + "=OLD." + colname
		sep = ","
	}
	p.MigrateRaw(migration, qry)
}

func (p *Migrator)DropAuditTrigger(migration string, origtable string){
	triggername := "audit_" + strings.ToLower(origtable)
	qry := `DROP TRIGGER ` + triggername
	p.MigrateRaw(migration + "drop-trigger", qry)
}



