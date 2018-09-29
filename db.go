package main

import (
	"encoding/json"
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
	tx       *sql.Tx
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
		fmt.Println("Initializing output db!")
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
		fmt.Println("Initializing input db!")
		err := InitInputDatabase(path)
		if err != nil {
			fmt.Println("Failed to initialize the database: ", err.Error())
			return nil, err
		}
	} else {
		fmt.Println("Input DB already initialized")
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

// GetBucketDefinitions is responsible for reading all of the bucket definitions
// out of the Destiny manifest database and returning the rows.
func (in *InputDB) GetBucketDefinitions() (*sql.Rows, error) {

	rows, err := in.Database.Query("SELECT * FROM DestinyInventoryBucketDefinition")

	return rows, err
}

// GetActivityModifierDefinitions is responsible for reading all of the modifiers
// out of the manifest database.
func (in *InputDB) GetActivityModifierDefinitions() (*sql.Rows, error) {

	rows, err := in.Database.Query("SELECT * FROM DestinyActivityModifierDefinition")

	return rows, err
}

// GetActivityTypeDefinitions is responsible for reading all of the modifiers
// out of the manifest database.
func (in *InputDB) GetActivityTypeDefinitions() (*sql.Rows, error) {

	rows, err := in.Database.Query("SELECT * FROM DestinyActivityTypeDefinition")

	return rows, err
}

// GetActivityModeDefinitions is responsible for reading all of the activity modes
// out of the manifest database.
func (in *InputDB) GetActivityModeDefinitions() (*sql.Rows, error) {

	rows, err := in.Database.Query("SELECT * FROM DestinyActivityModeDefinition")

	return rows, err
}

// GetActivityDefinitions is responsible for reading all of the activities
// out of the manifest database.
func (in *InputDB) GetActivityDefinitions() (*sql.Rows, error) {

	rows, err := in.Database.Query("SELECT * FROM DestinyActivityDefinition")

	return rows, err
}

// GetDestinationDefinitions is responsible for reading all of the destinations
// out of the manifest database.
func (in *InputDB) GetDestinationDefinitions() (*sql.Rows, error) {

	rows, err := in.Database.Query("SELECT * FROM DestinyDestinationDefinition")

	return rows, err
}

// GetPlaceDefinitions is responsible for reading all of the places
// out of the manifest database.
func (in *InputDB) GetPlaceDefinitions() (*sql.Rows, error) {

	rows, err := in.Database.Query("SELECT * FROM DestinyPlaceDefinition")

	return rows, err
}

// GetGearAssetsDefinition will request all rows from the assets table and return the rows.
func (in *InputDB) GetGearAssetsDefinition() (*sql.Rows, error) {

	rows, err := in.Database.Query("SELECT * FROM DestinyGearAssetsDefinition")

	return rows, err
}

// SaveManifestChecksum is responsible for persisting the checksum for the
// specified locale to be used for caching later.
func (out *OutputDB) SaveManifestChecksum(locale, checksum string) error {

	_, err := out.Database.Exec("UPDATE manifest_checksums SET md5=$1 WHERE locale=$2", checksum, locale)
	return err
}

func (out *OutputDB) OpenTransaction() error {

	var err error
	out.tx, err = out.Database.Begin()
	return err
}

func (out *OutputDB) CommitTransaction() error {

	return out.tx.Commit()
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

	stmt, err := out.Database.Prepare("INSERT INTO " + newTableTempName + " (item_hash, item_name, item_type, item_type_name, tier_type, tier_type_name, class_type, equippable, max_stack_size, display_source, non_transferrable, bucket_type_hash, icon) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)")
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
		_, err = txStmt.Exec(def.ItemHash, strings.ToLower(def.Name), def.ItemType, strings.ToLower(def.ItemTypeName), def.Inventory.TierType, strings.ToLower(def.Inventory.TierTypeName), def.ClassType, def.Equippable, def.Inventory.MaxStackSize, def.DisplaySource, def.NonTransferrable, def.Inventory.BucketTypeHash, def.Icon)
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

// DumpNewBucketDefinitions will take all of the bucket definitions parsed out of the latest
// manifest from Bungie and write them to the output database to be used for bucket lookups later.
func (out *OutputDB) DumpNewBucketDefintions(locale, checksum string, definitions []*BucketDefinition) error {

	newTableTempName := fmt.Sprintf("buckets_%s", checksum)

	// Inside a transaction we need to Insert all bucket definitions into a new DB and then rename the old db, rename the new one, delete the old one.
	// TODO: https://dba.stackexchange.com/questions/100779/how-to-atomically-replace-table-data-in-postgresql

	// Create temp new table
	out.Database.Exec("CREATE TABLE " + newTableTempName + "(LIKE \"buckets\")")
	out.Database.Exec("ALTER TABLE " + newTableTempName + " ADD PRIMARY KEY (bucket_hash)")

	stmt, err := out.Database.Prepare("INSERT INTO " + newTableTempName + " (bucket_hash, name,description) VALUES($1, $2, $3)")
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
		if def.Name == "" {
			continue
		}

		_, err = txStmt.Exec(def.BucketHash, strings.ToLower(def.DisplayProperties.Name),
			def.DisplayProperties.Description)
		if err != nil {
			fmt.Println("Error inserting bucket definition: ", err.Error())
		}
	}

	// Rename existing table with _old suffix
	tx.Exec("ALTER TABLE \"buckets\" RENAME TO \"buckets_old\"")

	// Rename new temp table with permanent table name
	tx.Exec("ALTER TABLE " + newTableTempName + " RENAME TO \"buckets\"")

	// Drop old table
	tx.Exec("DROP TABLE \"buckets_old\"")

	// Commit or Rollback if there were errors
	err = tx.Commit()
	if err != nil {
		fmt.Println("Error commiting transaction for inserting new bucket definitions: ", err.Error())
		return err
	}

	return nil
}

// DumpNewActivityModifierDefinitions will take all of the bucket definitions parsed
// out of the latest manifest from Bungie and write them to the output database to be
// used for bucket lookups later.
func (out *OutputDB) DumpNewActivityModifierDefinitions(locale, checksum string, definitions []*ActivityModifierDefinition) error {

	newTableTempName := fmt.Sprintf("activity_modifiers_%s", checksum)

	// Inside a transaction we need to Insert all bucket definitions into a new DB and then
	// rename the old db, rename the new one, delete the old one.
	// TODO: https://dba.stackexchange.com/questions/100779/how-to-atomically-replace-table-data-in-postgresql

	// Create temp new table
	out.Database.Exec("CREATE TABLE " + newTableTempName + "(LIKE \"activity_modifiers\")")
	out.Database.Exec("ALTER TABLE " + newTableTempName + " ADD PRIMARY KEY (hash)")

	stmt, err := out.Database.Prepare("INSERT INTO " + newTableTempName + " (hash, name, description) VALUES($1, $2, $3)")
	if err != nil {
		fmt.Println("Error preparing insert statement: ", err.Error())
		return err
	}

	// Insert all rows into the temp new table
	txStmt := out.tx.Stmt(stmt)
	for _, def := range definitions {
		if def.DisplayProperties.Name == "" {
			continue
		}

		_, err = txStmt.Exec(def.Hash, strings.ToLower(def.DisplayProperties.Name),
			def.DisplayProperties.Description)
		if err != nil {
			fmt.Println("Error inserting activity modifier definition: ", err.Error())
		}
	}

	// Rename existing table with _old suffix
	out.tx.Exec("ALTER TABLE \"activity_modifiers\" RENAME TO \"activity_modifiers_old\"")

	// Rename new temp table with permanent table name
	out.tx.Exec("ALTER TABLE " + newTableTempName + " RENAME TO \"activity_modifiers\"")

	// Drop old table
	out.tx.Exec("DROP TABLE \"activity_modifiers_old\"")

	return nil
}

// DumpNewActivityTypeDefinitions will take all of the activity type definitions parsed
// out of the latest manifest from Bungie and write them to the output database to be
// used for activity type lookups later.
func (out *OutputDB) DumpNewActivityTypeDefinitions(locale, checksum string, definitions []*ActivityTypeDefinition) error {

	newTableTempName := fmt.Sprintf("activity_types_%s", checksum)

	// Inside a transaction we need to Insert all bucket definitions into a new DB and then
	// rename the old db, rename the new one, delete the old one.
	// TODO: https://dba.stackexchange.com/questions/100779/how-to-atomically-replace-table-data-in-postgresql

	// Create temp new table
	out.Database.Exec("CREATE TABLE " + newTableTempName + "(LIKE \"activity_types\")")
	out.Database.Exec("ALTER TABLE " + newTableTempName + " ADD PRIMARY KEY (hash)")

	stmt, err := out.Database.Prepare("INSERT INTO " + newTableTempName + " (hash, name, description) VALUES($1, $2, $3)")
	if err != nil {
		fmt.Println("Error preparing insert statement: ", err.Error())
		return err
	}

	// Insert all rows into the temp new table
	txStmt := out.tx.Stmt(stmt)
	for _, def := range definitions {
		if def.DisplayProperties.Name == "" {
			continue
		}

		_, err = txStmt.Exec(def.Hash, strings.ToLower(def.DisplayProperties.Name),
			def.DisplayProperties.Description)
		if err != nil {
			fmt.Println("Error inserting activity type definition: ", err.Error())
		}
	}

	// Rename existing table with _old suffix
	_, err = out.tx.Exec("ALTER TABLE \"activity_types\" RENAME TO \"activity_types_old\"")
	if err != nil {
		fmt.Println("Error inserting activity type definition: ", err.Error())
	}

	// Rename new temp table with permanent table name
	_, err = out.tx.Exec("ALTER TABLE " + newTableTempName + " RENAME TO \"activity_types\"")
	if err != nil {
		fmt.Println("Error inserting activity type definition: ", err.Error())
	}

	// Drop old table
	_, err = out.tx.Exec("DROP TABLE \"activity_types_old\"")
	if err != nil {
		fmt.Println("Error inserting activity type definition: ", err.Error())
	}

	return nil
}

// DumpNewActivityModeDefinitions will take all of the activity mode definitions parsed
// out of the latest manifest from Bungie and write them to the output database to be
// used for activity type lookups later.
func (out *OutputDB) DumpNewActivityModeDefinitions(locale, checksum string, definitions []*ActivityModeDefintion) error {

	newTableTempName := fmt.Sprintf("activity_modes_%s", checksum)

	// Inside a transaction we need to Insert all bucket definitions into a new DB and then
	// rename the old db, rename the new one, delete the old one.
	// TODO: https://dba.stackexchange.com/questions/100779/how-to-atomically-replace-table-data-in-postgresql

	// Create temp new table
	out.Database.Exec("CREATE TABLE " + newTableTempName + "(LIKE \"activity_modes\")")
	out.Database.Exec("ALTER TABLE " + newTableTempName + " ADD PRIMARY KEY (hash)")

	stmt, err := out.Database.Prepare("INSERT INTO " + newTableTempName +
		" (hash, name, description, mode_type, category, tier, is_aggregate, is_team_based)" +
		" VALUES($1, $2, $3, $4, $5, $6, $7, $8)")
	if err != nil {
		fmt.Println("Error preparing insert statement: ", err.Error())
		return err
	}

	// Insert all rows into the temp new table
	txStmt := out.tx.Stmt(stmt)
	for _, def := range definitions {
		if def.DisplayProperties.Name == "" {
			continue
		}

		_, err = txStmt.Exec(def.Hash, strings.ToLower(def.DisplayProperties.Name),
			def.DisplayProperties.Description, def.ModeType, def.Category, def.Tier, def.IsAggregate, def.IsTeamBased)
		if err != nil {
			fmt.Println("Error inserting activity mode definition: ", err.Error())
		}
	}

	// Rename existing table with _old suffix
	_, err = out.tx.Exec("ALTER TABLE \"activity_modes\" RENAME TO \"activity_modes_old\"")
	if err != nil {
		fmt.Println("Error inserting activity mode definition: ", err.Error())
	}

	// Rename new temp table with permanent table name
	_, err = out.tx.Exec("ALTER TABLE " + newTableTempName + " RENAME TO \"activity_modes\"")
	if err != nil {
		fmt.Println("Error inserting activity mode definition: ", err.Error())
	}

	// Drop old table
	_, err = out.tx.Exec("DROP TABLE \"activity_modes_old\"")
	if err != nil {
		fmt.Println("Error inserting activity mode definition: ", err.Error())
	}

	return nil
}

// DumpNewPlaceDefinitions will take all of the place definitions parsed
// out of the latest manifest from Bungie and write them to the output database to be
// used for activity type lookups later.
func (out *OutputDB) DumpNewPlaceDefinitions(locale, checksum string, definitions []*PlaceDefinition) error {

	newTableTempName := fmt.Sprintf("places_%s", checksum)

	// Inside a transaction we need to Insert all bucket definitions into a new DB and then
	// rename the old db, rename the new one, delete the old one.
	// TODO: https://dba.stackexchange.com/questions/100779/how-to-atomically-replace-table-data-in-postgresql

	// Create temp new table
	out.Database.Exec("CREATE TABLE " + newTableTempName + "(LIKE \"places\")")
	out.Database.Exec("ALTER TABLE " + newTableTempName + " ADD PRIMARY KEY (hash)")

	stmt, err := out.Database.Prepare("INSERT INTO " + newTableTempName + " (hash, name, description) VALUES($1, $2, $3)")
	if err != nil {
		fmt.Println("Error preparing insert statement: ", err.Error())
		return err
	}

	// Insert all rows into the temp new table
	txStmt := out.tx.Stmt(stmt)
	for _, def := range definitions {
		if strings.ToLower(def.Name) == "classified" {
			continue
		}

		_, err = txStmt.Exec(def.Hash, strings.ToLower(def.Name),
			def.DisplayProperties.Description)
		if err != nil {
			fmt.Println("Error inserting place definition: ", err.Error())
		}
	}

	// Rename existing table with _old suffix
	out.tx.Exec("ALTER TABLE \"places\" RENAME TO \"places_old\"")

	// Rename new temp table with permanent table name
	out.tx.Exec("ALTER TABLE " + newTableTempName + " RENAME TO \"places\"")

	// Drop old table
	out.tx.Exec("DROP TABLE \"places_old\"")

	return nil
}

// DumpNewDestinationDefinitions will take all of the destination definitions parsed
// out of the latest manifest from Bungie and write them to the output database to be
// used for activity type lookups later.
func (out *OutputDB) DumpNewDestinationDefinitions(locale, checksum string, definitions []*DestinationDefintion) error {

	newTableTempName := fmt.Sprintf("destinations_%s", checksum)

	// Inside a transaction we need to Insert all bucket definitions into a new DB and then
	// rename the old db, rename the new one, delete the old one.
	// TODO: https://dba.stackexchange.com/questions/100779/how-to-atomically-replace-table-data-in-postgresql

	// Create temp new table
	out.Database.Exec("CREATE TABLE " + newTableTempName + "(LIKE \"destinations\")")
	out.Database.Exec("ALTER TABLE " + newTableTempName + " ADD PRIMARY KEY (hash)")

	stmt, err := out.Database.Prepare("INSERT INTO " + newTableTempName + " (hash, name, description, place_hash) VALUES($1, $2, $3, $4)")
	if err != nil {
		fmt.Println("Error preparing insert statement: ", err.Error())
		return err
	}

	// Insert all rows into the temp new table
	txStmt := out.tx.Stmt(stmt)
	for _, def := range definitions {
		// NOTE: Apparently this might be acceptable a lot of destinations have no name
		// if def.Name == "" {
		// 	continue
		// }

		_, err = txStmt.Exec(def.Hash, strings.ToLower(def.DisplayProperties.Name),
			def.DisplayProperties.Description, def.PlaceHash)
		if err != nil {
			fmt.Println("Error inserting destination definition: ", err.Error())
		}
	}

	// Rename existing table with _old suffix
	out.tx.Exec("ALTER TABLE \"destinations\" RENAME TO \"destinations_old\"")

	// Rename new temp table with permanent table name
	out.tx.Exec("ALTER TABLE " + newTableTempName + " RENAME TO \"destinations\"")

	// Drop old table
	out.tx.Exec("DROP TABLE \"destinations_old\"")

	return nil
}

// DumpNewActivityDefinitions will take all of the activity definitions parsed out of the manifest
// and write them to a new table that is a flattened out version of the JSON in the sqlite database.
func (out *OutputDB) DumpNewActivityDefinitions(locale, checksum string, definitions []*ActivityDefinition) error {

	newTableTempName := fmt.Sprintf("activities_%s", checksum)

	// Inside a transaction we need to Insert all bucket definitions into a new DB and then
	// rename the old db, rename the new one, delete the old one.
	// TODO: https://dba.stackexchange.com/questions/100779/how-to-atomically-replace-table-data-in-postgresql

	// Create temp new table
	out.Database.Exec("CREATE TABLE " + newTableTempName + "(LIKE \"activities\")")
	out.Database.Exec("ALTER TABLE " + newTableTempName + " ADD PRIMARY KEY (hash)")

	stmt, err := out.Database.Prepare("INSERT INTO " + newTableTempName +
		" (hash, name, description, light_level, destination_hash, place_hash, activity_type_hash, is_playlist," +
		" is_pvp, direct_activity_mode_hash, direct_activity_mode_type, activity_mode_hashes, activity_mode_types, " +
		" rewards, modifiers, challenges, matchmaking)" +
		" VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)")
	if err != nil {
		fmt.Println("Error preparing insert statement: ", err.Error())
		return err
	}

	// Insert all rows into the temp new table
	txStmt := out.tx.Stmt(stmt)
	for _, def := range definitions {
		if def.Name == "" || strings.ToLower(def.Name) == "classified" {
			continue
		}

		activityModeHashesBytes, err := json.Marshal(def.ActivityModeHashes)
		activityModeTypeBytes, err := json.Marshal(def.ActivityModeTypes)
		rewardsBytes, err := json.Marshal(def.Rewards)
		modifiersBytes, err := json.Marshal(def.Modifiers)
		challengesBytes, err := json.Marshal(def.Challenges)
		matchmakingBytes, err := json.Marshal(def.Matchmaking)

		_, err = txStmt.Exec(def.Hash, strings.ToLower(def.Name),
			def.Description, def.LightLevel, def.DestinationHash, def.PlaceHash, def.ActivityTypeHash,
			def.IsPlaylist, def.IsPVP, def.DirectActivityModeHash, def.DirectActivityModeType,
			string(activityModeHashesBytes), string(activityModeTypeBytes), string(rewardsBytes),
			string(modifiersBytes), string(challengesBytes), string(matchmakingBytes))

		if err != nil {
			fmt.Println("Error inserting activity definition: ", err.Error())
		}
	}

	// Rename existing table with _old suffix
	out.tx.Exec("ALTER TABLE \"activities\" RENAME TO \"activities_old\"")

	// Rename new temp table with permanent table name
	out.tx.Exec("ALTER TABLE " + newTableTempName + " RENAME TO \"activities\"")

	// Drop old table
	out.tx.Exec("DROP TABLE \"activities_old\"")

	return nil
}

func (out *OutputDB) RemoveActivityConstraints() {

	out.tx.Exec("ALTER TABLE activities DROP CONSTRAINT destination_hash_fkey")
	out.tx.Exec("ALTER TABLE activities DROP CONSTRAINT place_hash_fkey")
	out.tx.Exec("ALTER TABLE activities DROP CONSTRAINT activity_type_hash_fkey")
	//out.tx.Exec("ALTER TABLE activities DROP CONSTRAINT direct_activity_mode_hash_fkey")
	// out.tx.Exec("ALTER TABLE activities DROP CONSTRAINT direct_activity_mode_type_fkey")
}

func (out *OutputDB) AddActivityConstraints() {

	var err error
	_, err = output.tx.Exec("ALTER TABLE activities ADD CONSTRAINT destination_hash_fkey FOREIGN KEY (destination_hash) REFERENCES destinations (hash)")
	if err != nil {
		panic(err)
	}
	_, err = output.tx.Exec("ALTER TABLE activities ADD CONSTRAINT place_hash_fkey FOREIGN KEY (place_hash) REFERENCES places (hash)")
	if err != nil {
		panic(err)
	}
	_, err = output.tx.Exec("ALTER TABLE activities ADD CONSTRAINT activity_type_hash_fkey FOREIGN KEY (activity_type_hash) REFERENCES activity_types (hash)")
	if err != nil {
		panic(err)
	}
	// _, err = output.tx.Exec("ALTER TABLE activities ADD CONSTRAINT direct_activity_mode_hash_fkey FOREIGN KEY (direct_activity_mode_hash) REFERENCES activity_modes (hash)")
	// if err != nil {
	// 	panic(err)
	// }
	// _, err = output.tx.Exec("ALTER TABLE activities ADD CONSTRAINT direct_activity_mode_type_fkey FOREIGN KEY (direct_activity_mode_type) REFERENCES activity_modes (mode_type)")
	// if err != nil {
	// 	panic(err)
	// }
}

func (out *OutputDB) DumpGearAssetsDefinitions(definitions []*ManifestRow) error {

	out.Database.Exec("DELETE FROM assets WHERE true")

	stmt, err := out.Database.Prepare("INSERT INTO assets (id, json) VALUES ($1, $2)")
	if err != nil {
		return err
	}

	for _, def := range definitions {
		_, err = stmt.Exec(uint32(def.ID), def.JSON)

		if err != nil {
			fmt.Printf("Failed to insert row into assets defintiions: %s\n", err.Error())
		}
	}

	return nil
}
