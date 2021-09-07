package migrator

import (
	"log"
	"strings"
	"database/sql"
)

type Migrator struct{
	db *sql.DB
	migrationsmap map[string]bool
}

func NewMigrator(db *sql.DB)*Migrator{
	return &Migrator{
		db: db,
	}
}


//columns is a map of column name to specification
func (p *Migrator)CreateTable(
	name string, columns map[string]string, indexes []string, primarykeys []string){

	stmt := "Create table If not Exists " + name + " ("
	sep := ""
	for colname, coltype := range columns {
		stmt += sep + colname + " " + coltype
		sep = ", "
	}
	for _, indexname := range indexes {
		stmt += sep + "index(" + indexname + ")"
		sep = ", "
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
		panic("Failed to create table")
	}
}

func (p *Migrator)AlterTableAdd(name string, columns map[string]string){
	stmt := "Alter table " + name + " add ("
	sep := ""
	for colname, coltype := range columns {
		stmt += sep + colname + " " + coltype
		sep = ", "
	}
	stmt += ")"
	_, err := p.db.Exec(stmt)
	if err != nil {
		log.Println("Failed to create " + name  + " table with error " + err.Error())
		panic("Failed to create table")
	}
}

func (p *Migrator)Migrate(migration string, tablename string, columns map[string]string, indexes []string, primarykeys []string){
	if _, ok := p.migrationsmap[migration]; !ok {
		///this migration is new
		p.CreateTable(tablename, columns, indexes, primarykeys)
		_,err := p.db.Exec("Insert into migrations set migrations=?", migration)
		if err != nil {
			log.Println("Failed to create " + tablename  + " table with error " + err.Error())
			panic("Failed to create table")
		}
		log.Println("Migrated " + migration)
	}
}

func (p *Migrator)markMigration(migration string){
	_,err := p.db.Exec("Insert into migrations set migrations=?", migration)
	if err != nil {
		log.Println("Failed to set migration " + migration  + " table with error " + err.Error())
		panic("Failed to create table")
	}
	log.Println("Migrated " + migration)
}

func (p *Migrator)Alter(migration string, tablename string, columns map[string]string){
	if _, ok := p.migrationsmap[migration]; !ok {
		///this migration is new
		p.AlterTableAdd(tablename, columns)
		_,err := p.db.Exec("Insert into migrations set migrations=?", migration)
		if err != nil {
			log.Println("Failed to create " + tablename  + " table with error " + err.Error())
			panic("Failed to create table")
		}
		log.Println("Migrated " + migration)
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
				panic("Failed to create table")
			}
		}

		_,err := p.db.Exec("Insert into migrations set migrations=?", migration)
		if err != nil {
			log.Println("Failed to create " + tablename  + " table with error " + err.Error())
			panic("Failed to create table")
		}
		log.Println("Migrated " + migration)
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
		_, err := p.db.Exec("alter table " + tablename + " drop primary key, add primary key(" + allkeys + ")")
		if err != nil {
			log.Println("Failed to alter primary keys ", err)
			panic("Failed to alter primary keys")
		}
		p.markMigration(migration)
	}
}

func (p *Migrator)DropPrimes(migration string, tablename string){
	if _, ok := p.migrationsmap[migration]; !ok {
		_, err := p.db.Exec("alter table " + tablename + " drop primary key")
		if err != nil {
			log.Println("Failed to drop primary keys ", err)
			panic("Failed to drop primary keys")
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
			panic("Failed to alter primary keys")
		}
		p.markMigration(migration)
	}
}


func (p *Migrator)MigrateRaw(migration string, qry string){
	if _, ok := p.migrationsmap[migration]; !ok {
		_, err := p.db.Exec(qry)
		if err != nil {
			log.Panic("Failed to run migration raw ", err, " in query ", qry)
			panic("Failed to run migration raw")
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



