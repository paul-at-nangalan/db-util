package batch

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/paul-at-nangalan/errorhandler/handlers"
	"log"
)

///NOT transactional
/// Just aimed at batching stuff up to reduce rounds trips to the DB
/**
Usage

Create a writer:

...

	writer := NewWriter(db, "mytable", []string{"field1", "field2", ...},
				25, `ON CONFLICT (field1) DO UPDATE SET field2 = excluded.field2 ...`)
...

	//// Use the writer
	defer writer.Flush()

	for .... {
		writer.Exec(val1, val2, .... ) ////Make sure the number of fields match the col names
	}
 */
type Writer struct{
	batchstmt *sql.Stmt
	singlstmt *sql.Stmt

	numfields int
	batchsize int

	cache []interface{}
}

func genQry(tablename string, colnames []string, batchsize int, ondupkey string)string{

	batchqry := `INSERT INTO ` + tablename + `(`
	del := ""
	for _, colname := range colnames{
		batchqry += del + colname
		del = ","
	}
	batchqry += `) VALUES `
	del = ""
	for i := 0; i < batchsize; i++{
		innerdel := ""
		batchqry += del + `(`
		for x, _ := range colnames{
			batchqry += innerdel
			batchqry += fmt.Sprintf("$%d", (i * len(colnames)) + x + 1)
			innerdel = ","
		}
		batchqry += `)`
		del = ","
	}
	batchqry += " " + ondupkey
	return batchqry
}

func NewWriter(db *sql.DB, tablename string, colnames []string, ondupkeyclause string,
	batchsize int)*Writer{
	batchqry := genQry(tablename, colnames, batchsize, ondupkeyclause)
	///single query is just a special case of multiple
	singleqry := genQry(tablename, colnames, 1, ondupkeyclause)

	fmt.Println(batchqry)
	batchstmt, err := db.Prepare(batchqry)
	handlers.PanicOnError(err)
	singlestmt, err := db.Prepare(singleqry)
	return &Writer{
		batchsize: batchsize,
		batchstmt: batchstmt,
		singlstmt: singlestmt,
		cache: make([]interface{}, 0, batchsize * len(colnames)),
		numfields: len(colnames),
	}
}

func (p *Writer)Exec(vals ...interface{})(res sql.Result, err error){
	if len(vals) != p.numfields{
		return nil, errors.New(fmt.Sprint("Mismatch number of columns, expected ", p.numfields,
			" It should match the number of column names were passed to NewWriter(...)"))
	}

	p.cache = append(p.cache, vals...)
	if len(p.cache) == p.batchsize * p.numfields{
		res, err = p.batchstmt.Exec(p.cache...)
		p.cache = p.cache[0:0]
	}
	if len(p.cache) > p.batchsize * p.numfields{
		log.Panic("Oops, somethings wrong ", len(p.cache), " > ", p.batchsize * p.numfields)
	}
	return res, err
}

func (p *Writer)Flush()(res sql.Result, err error){
	vals := make([]interface{}, p.numfields)
	for i, val := range p.cache{
		//fmt.Println("Flush, i ", i, " indx ", i % p.numfields)
		vals[i % p.numfields] = val
		if i % p.numfields == len(vals) - 1{
			//fmt.Println("Exec ", vals)
			res, err = p.singlstmt.Exec(vals...)
			if err != nil{
				return nil, err
			}
		}
	}
	return res, err
}