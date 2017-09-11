package main

import (
	"fmt"
	"os"
	"strings"

	"database/sql"

	_ "github.com/lib/pq" // Only want to import the interface here
	_ "github.com/mattn/go-sqlite3"
)

// OutputDB represents the database that the parsed definitions will be saved
type OutputDB struct {
	Database *sql.DB
}

// InputDB represents the input database pulled down from Bungie
type InputDB struct {
	Database *sql.DB
}

var input *InputDB
var output *OutputDB

// InitOutputDatabase is in charge of preparing any Statements that will be commonly used as well
// as setting up the database connection pool.
func InitOutputDatabase() error {

	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Println("DB errror: ", err.Error())
		return err
	}

	output = &OutputDB{
		Database: db,
	}

	return nil
}

// InitInputDatabase is in charge of preparing any Statements that will be commonly used as well
// as setting up the database connection pool.
func InitInputDatabase(dbPath string) error {

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		fmt.Println("DB errror: ", err.Error())
		return err
	}

	input = &InputDB{
		Database: db,
	}

	return nil
}

// GetOutputDBConnection is a helper for getting a connection to the DB based on
// environment variables or some other method.
func GetOutputDBConnection() (*OutputDB, error) {

	if output == nil {
		fmt.Println("Initializing db!")
		err := InitOutputDatabase()
		if err != nil {
			fmt.Println("Failed to initialize the database: ", err.Error())
			return nil, err
		}
	}

	return output, nil
}

// GetInputDBConnection is a helper for getting a connection to the DB based on
// environment variables or some other method.
func GetInputDBConnection(path string) (*InputDB, error) {

	if input == nil {
		fmt.Println("Initializing db!")
		err := InitInputDatabase(path)
		if err != nil {
			fmt.Println("Failed to initialize the database: ", err.Error())
			return nil, err
		}
	}

	return input, nil
}

// ReadManifestChecksums will pull the checksums for the previously parsed
// manifests to be used for caching. If the manifest checksums match,
// there have not been any changes.
func ReadManifestChecksums() (map[string]string, error) {

	out, err := GetOutputDBConnection()
	if err != nil {
		fmt.Println("Error getting output database connection to read manifest checksums: ", err.Error())
		return nil, err
	}

	result := make(map[string]string)

	rows, err := out.Database.Query("SELECT * FROM manifest_checksums")
	if err != nil {
		fmt.Println("Error reading checksums from database: ", err.Error())
		return nil, err
	}

	for rows.Next() {
		var locale string
		var md5 string

		rows.Scan(&locale, &md5)

		result[locale] = md5
	}

	return result, nil
}

// GetItemDefinitions will pull all rows from the input database table
// with all of the item defintions.
func (in *InputDB) GetItemDefinitions() (*sql.Rows, error) {

	rows, err := in.Database.Query("SELECT * FROM DestinyInventoryItemDefinition")

	return rows, err
}

// SaveManifestChecksum is responsible for persisting the checksum for the
// specified locale to be used for caching later.
func (out *OutputDB) SaveManifestChecksum(locale, checksum string) error {

	_, err := out.Database.Exec("UPDATE manifest_checksums SET md5=$1 WHERE locale=$2", checksum, locale)
	return err
}

// DumpNewItemDefinitions will take all of the item definitions parsed out of the latest
// manifest from Bungie and write them to the output database to be used for item lookups later.
func (out *OutputDB) DumpNewItemDefintions(locale, checksum string, definitions []*ItemDefinition) error {

	newTableTempName := fmt.Sprintf("items_%s", checksum)

	// Inside a transaction we need to Insert all item definitions into a new DB and then rename the old db, rename the new one, delete the old one.
	// TODO: https://dba.stackexchange.com/questions/100779/how-to-atomically-replace-table-data-in-postgresql

	// Create temp new table
	out.Database.Exec("CREATE TABLE " + newTableTempName + "(LIKE \"items\")")
	out.Database.Exec("ALTER TABLE " + newTableTempName + " ADD PRIMARY KEY (item_hash)")

	stmt, err := out.Database.Prepare("INSERT INTO " + newTableTempName + " (item_hash, item_name, item_type, item_type_name, tier_type, tier_type_name, class_type, equippable, max_stack_size, display_source, non_transferrable, bucket_type_hash) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)")
	if err != nil {
		fmt.Println("Error preparing insert statement: ", err.Error())
		return err
	}
	tx, err := out.Database.Begin()
	if err != nil {
		fmt.Println("Error opening transaction to output DB: ", err.Error())
		return err
	}

	// Insert all rows into the temp new table
	txStmt := tx.Stmt(stmt)
	for _, def := range definitions {
		_, err = txStmt.Exec(def.ItemHash, strings.ToLower(def.DisplayProperties.ItemName), def.ItemType, strings.ToLower(def.ItemTypeName), def.Inventory.TierType, strings.ToLower(def.Inventory.TierTypeName), def.ClassType, def.Equippable, def.Inventory.MaxStackSize, def.DisplaySource, def.NonTransferrable, def.Inventory.BucketTypeHash)
		if err != nil {
			fmt.Println("Error inserting item definition: ", err.Error())
		}
	}

	// Rename existing table with _old suffix
	tx.Exec("ALTER TABLE \"items\" RENAME TO \"items_old\"")

	// Rename new temp table with permanent table name
	tx.Exec("ALTER TABLE " + newTableTempName + " RENAME TO \"items\"")

	// Drop old table
	tx.Exec("DROP TABLE \"items_old\"")

	// Commit or Rollback if there were errors
	err = tx.Commit()
	if err != nil {
		fmt.Println("Error commiting transaction for inserting new item definitions: ", err.Error())
		return err
	}

	return nil
}
