# db-util
Very basic utility to set up DB tables

## Import

```
    import "github.com/paul-at-nangalan/db-util/migrator"
```

## Example usage

```
	mig := migrator.NewMigrator(db)

	cols := map[string]string{
		"Id": "BIGSERIAL",
		"UploadId": "BIGINT",
		"CID": "text",
		"Date": "TIMESTAMP",
		"Ticker": "text",
		"Units": "DOUBLE",
		"PricePerUnit": "DOUBLE",
		"Value": "DOUBLE",
		"Currency": "text",
	}
	primes := []string{"Id"}
	indexes := []string{"CID", "UploadId"}
	mig.Migrate("create-vals-table", "vals",
		cols, indexes, primes)
```
