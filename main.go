package main

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

const (
	// BaseURL is the base of all URLs for requesting data from Bungie.
	BaseURL = "https://www.bungie.net"
	// ManifestURL is the URL for the manifest spec that will point to the individual manifest DBs.
	ManifestURL = BaseURL + "/Platform/Destiny2/Manifest/"
)

// ManifestSpecResponse is the response from the public manifest endpoint provided by Bungie.
// It provides links to all of the content files which are zipped SQLite databases.
type ManifestSpecResponse struct {
	Response struct {
		Version                  string
		MobileAssetContentPath   string
		MobileGearAssetDataBases []struct {
			Version int
			Path    string
		}
		MobileWorldContentPaths map[string]string
		MobileGearCDN           map[string]string
	}
	ErrorCode       int
	ThrottleSeconds int
	ErrorStatus     string
	Message         string
}

// ManifestRow represents a single row from the input manifest database
type ManifestRow struct {
	ID   int
	JSON string
}

// DisplayProperties represents almost any piece of the API that is
// intended to be displayed on the screen in some way.
type DisplayProperties struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Icon        string `json:"icon"`
}

// ItemDefinition stores all of the fields from the DestinyInventoryItemDefinitions table
// that we are concerned with.
type ItemDefinition struct {
	ItemHash         int    `json:"hash"`
	ItemType         int    `json:"itemType"`
	ItemTypeName     string `json:"itemTypeDisplayName"`
	ClassType        int    `json:"classType"`
	Equippable       bool   `json:"equippable"`
	DisplaySource    string `json:"displaySource"`
	NonTransferrable bool   `json:"nonTransferrable"`
	Inventory        struct {
		TierType       int    `json:"tierType"`
		TierTypeName   string `json:"tierTypeName"`
		MaxStackSize   int    `json:"maxStackSize"`
		BucketTypeHash int64  `json:"bucketTypeHash"`
	} `json:"inventory"`
	*DisplayProperties `json:"displayProperties"`
}

type BucketDefinition struct {
	BucketHash         int `json:"hash"`
	*DisplayProperties `json:"displayProperties"`
}

type ActivityModifierDefinition struct {
	Hash               uint `json:"hash"`
	*DisplayProperties `json:"displayProperties"`
}

type ActivityTypeDefinition struct {
	Hash               uint `json:"hash"`
	*DisplayProperties `json:"displayProperties"`
}

type ActivityModeDefintion struct {
	Hash               uint `json:"hash"`
	ModeType           int  `json:"modeType"`
	Category           int  `json:"activityModeCategory"`
	Tier               int  `json:"tier"`
	IsAggregate        bool `json:"isAggregateMode"`
	IsTeamBased        bool `json:"isTeamBased"`
	*DisplayProperties `json:"displayProperties"`
}

type PlaceDefinition struct {
	Hash               uint `json:"hash"`
	*DisplayProperties `json:"displayProperties"`
}

type DestinationDefintion struct {
	Hash               uint `json:"hash"`
	PlaceHash          uint `json:"placeHash"`
	*DisplayProperties `json:"displayProperties"`
}

/* Remove constraints
ALTER TABLE activities DROP CONSTRAINT destination_hash_fkey;
ALTER TABLE activities DROP CONSTRAINT place_hash_fkey;
ALTER TABLE activities DROP CONSTRAINT activity_type_hash_fkey;
ALTER TABLE activities DROP CONSTRAINT direct_activity_mode_hash_fkey;
ALTER TABLE activities DROP CONSTRAINT direct_activity_mode_type_fkey;
*/

/*
CREATE TABLE activities (
	hash bigint PRIMARY KEY,
	name text NOT NULL,
	description text NOT NULL,

	light_level integer,
	destination_hash bigint,
	place_hash bigint,
	activity_type_hash bigint,
	is_playlist boolean DEFAULT FALSE,
	is_pvp boolean DEFAULT FALSE,
	direct_activity_mode_hash bigint,
	direct_activity_mode_type integer,
	activity_mode_hashes json,
	activity_mode_types json,
	rewards json,
	modifiers json,
	challenges json,
	matchmaking json
);
*/

/* Add constraints
ALTER TABLE activities ADD CONSTRAINT destination_hash_fkey FOREIGN KEY (destination_hash) REFERENCES destinations (hash);
ALTER TABLE activities ADD CONSTRAINT place_hash_fkey FOREIGN KEY (place_hash) REFERENCES places (hash);
ALTER TABLE activities ADD CONSTRAINT activity_type_hash_fkey FOREIGN KEY (activity_type_hash) REFERENCES activity_types (hash);
ALTER TABLE activities ADD CONSTRAINT direct_activity_mode_hash_fkey FOREIGN KEY (direct_activity_mode_hash) REFERENCES activity_modes (hash)
ALTER TABLE activities ADD CONSTRAINT direct_activity_mode_type_fkey FOREIGN KEY (direct_activity_mode_type) REFERENCES activity_modes (mode_type);
*/

type ActivityDefinition struct {
	Hash                   uint   `json:"hash"`
	LightLevel             uint   `json:"activityLightLevel"`
	DestinationHash        uint   `json:"destinationHash"`
	PlaceHash              uint   `json:"placeHash"`
	ActivityTypeHash       uint   `json:"activityTypeHash"`
	IsPlaylist             bool   `json:"isPlaylist"`
	IsPVP                  bool   `json:"isPvP"`
	DirectActivityModeHash uint   `json:"directActivityModeHash"`
	DirectActivityModeType int    `json:"directActivityModeType"`
	ActivityModeHashes     []uint `json:"activityModeHashes"`
	ActivityModeTypes      []int  `json:"activityModeTypes"`

	// TODO: Find the actual types for these, not just interface{}
	Rewards []*struct {
		RewardItems *struct {
			ItemHash uint `json:"itemHash"`
			Quantity uint `json:"quantity"`
		} `json:"rewardItems"`
	} `json:"rewards"`
	Modifiers []*struct {
		ActivityModifierHash uint `json:"activityModifierHash"`
	} `json:"modifiers"`
	Challenges []*struct {
		RewardSiteHash           uint `json:"rewardSiteHash"`
		InhibitRewardsUnlockHash uint `json:"inhibitRewardsUnlockHash"`
		ObjectiveHash            uint `json:"objectiveHash"`
	} `json:"challenges"`
	PlaylistItems []*struct {
		ActivityHash     uint `json:"activityHash"`
		ActivityModeHash uint `json:"activityModeHash"`
		Weight           uint `json:"weight"`
	} `json:"playlistItems"`

	Matchmaking *struct {
		IsMatchmade bool `json:"isMatchmade"`
		MinParty    int  `json:"minParty"`
		MaxParty    int  `json:"maxParty"`
		MaxPlayers  int  `json:"maxPlayers"`
		// TODO: Not sure wth this is
		RequiresGuardianOath bool `json:"requiresGuardianOath"`
	} `json:"matchmaking"`
	*DisplayProperties `json:"displayProperties"`
}

func main() {

	withItems := flag.Bool("items", false, "Use this to request item entries be parsed from the manifest")
	withAssets := flag.Bool("assets", false, "Use this flag to request assets be parsed")
	limit := flag.Int("limit", -1, "Use to limit the number of items imported, useful for platforms where row limits are a thing on free tiers. (looking at you Heroku)")
	flag.Parse()

	fmt.Printf("Running version=(%s) build on date=(%s)\n", VERSION, BUILD_DATE)

	manifestSpec, err := readManifestSpec()
	if err != nil {
		fmt.Printf("Error requesting manfiest spec from Bungie: %s\n", err.Error())
		return
	}

	// manifestChecksums stores the md5sums of the manifest files keyed by the language they
	// are associated with.
	manifestChecksums, err := ReadManifestChecksums()
	if err != nil {
		fmt.Println("Error trying to read manifest checksums")
		return
	}

	if *withAssets {
		fmt.Println("Processing assets...")
		processAssets(manifestSpec)
	}
	if *withItems {
		fmt.Println("Processing items...")
		processItems(manifestSpec, manifestChecksums, *limit)
	}
}

// readManifestSpec will read the manifest spec that contains the URLs to the individual manifest
// databases. The databases are sent as zipped sqlite databases.
func readManifestSpec() (*ManifestSpecResponse, error) {
	client := http.DefaultClient
	req, _ := http.NewRequest("GET", ManifestURL, nil)

	bungieAPIKey := os.Getenv("BUNGIE_API_KEY")
	if bungieAPIKey != "" {
		// Providing an API Key will decrease the chances of the request being throttled
		req.Header.Add("X-Api-Key", bungieAPIKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("Failed to make request to Bungie for the manifest spec")
	}
	defer resp.Body.Close()

	manifestSpec := &ManifestSpecResponse{}
	err = json.NewDecoder(resp.Body).Decode(manifestSpec)
	if err != nil {
		return nil, err
	}

	if manifestSpec.ErrorStatus != "Success" || manifestSpec.Message != "Ok" {
		return nil, errors.New("Error response from Bungie for manifest spec")
	}

	return manifestSpec, nil
}

// checksumFromManifestFilename parses the md5sum value out of the manifest path
// example path: /common/destiny_content/sqlite/en/world_sql_content_7d6b460360f589e94baeb8308cada327.content
func checksumFromManifestFilename(path string) string {
	underscoreIndex := strings.LastIndex(path, "_")
	dotIndex := strings.LastIndex(path, ".")
	if dotIndex == -1 || underscoreIndex == -1 || dotIndex < underscoreIndex {
		return ""
	}

	return path[underscoreIndex+1 : dotIndex-1]
}

func processAssets(manifestSpec *ManifestSpecResponse) {

	zippedDBName := fmt.Sprintf("asset_sql_content.content")
	// TODO: This 2 here is hardcoded. the manifest spec has some weird entries for assets.
	// This should be removed later or possibly changed to a different index at some point.
	resourcePath := manifestSpec.Response.MobileGearAssetDataBases[2].Path
	sqlitePath := downloadZippedManifestDB(resourcePath, "", zippedDBName)
	fmt.Println(sqlitePath)
	defer os.Remove(sqlitePath)

	err := processGearAssetsManifestDB(sqlitePath)
	if err != nil {
		fmt.Printf("Error processing gear assets manifest DB: %s\n", err.Error())
		return
	}
}

func processGearAssetsManifestDB(sqlitePath string) error {

	in, err := GetInputDBConnection(sqlitePath)
	if err != nil {
		return err
	}
	//defer in.Database.Close()
	_, err = GetOutputDBConnection()
	if err != nil {
		return err
	}

	rows, err := in.GetGearAssetsDefinition()
	if err != nil {
		return err
	}
	defer rows.Close()

	manifestRows := make([]*ManifestRow, 0, 100)
	for rows.Next() {
		row := &ManifestRow{}
		err = rows.Scan(&row.ID, &row.JSON)
		if err != nil {
			fmt.Printf("Failed to scan input from assets table: %s\n", err.Error())
		}

		manifestRows = append(manifestRows, row)
	}

	err = output.DumpGearAssetsDefinitions(manifestRows)
	fmt.Printf("Processed %d asset definitions...\n", len(manifestRows))

	return err
}

func processItems(manifestSpec *ManifestSpecResponse, currentChecksums map[string]string, rowLimit int) {
	for lang, path := range manifestSpec.Response.MobileWorldContentPaths {
		if lang != "en" {
			// For now, only parsing the english items manifest
			continue
		}

		fmt.Println("Checking manifest for language: ", lang)
		currentChecksum := currentChecksums[lang]
		incomingChecksum := checksumFromManifestFilename(path)
		if currentChecksum != "" && currentChecksum == incomingChecksum {
			fmt.Println("Incoming manifest is the same as the current one already stored...skipping!")
			continue
		}

		zippedDBName := fmt.Sprintf("world_sql_content_%s.content", lang)
		sqlitePath := downloadZippedManifestDB(path, lang, zippedDBName)
		defer os.Remove(sqlitePath)

		err := processWorldContentsManifestDB(lang, incomingChecksum, sqlitePath, rowLimit)
		if err != nil {
			fmt.Printf("Failed to process items database for lang(%s): %s\n", lang, err.Error())
		}
	}
}

// parseMobileWorldContentPath will download and unzip the world content database,
// unzip it, and save the sqlite database somewhere on disk. The return value
// is the location on disk where the extracted sqlite database is saved.
func downloadZippedManifestDB(resourcePath, language, zippedDBName string) string {

	client := http.DefaultClient
	req, _ := http.NewRequest("GET", BaseURL+resourcePath, nil)

	strings.TrimSuffix(resourcePath, ".content")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Failed to download manifest for lanuage %s: %s\n", language, err.Error())
		return ""
	} else if resp.StatusCode == 304 {
		fmt.Printf("Manifest file for language %s has not changed... skipping!\n", language)
		return ""
	}
	defer resp.Body.Close()

	// Download the zipped content
	zipPath := zippedDBName
	output, err := os.OpenFile(zipPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Failed to open output file for writing: ", err.Error())
		return ""
	}
	defer output.Close()

	_, err = io.Copy(output, resp.Body)
	if err != nil {
		fmt.Println("Failed to write zipped content to disk: ", err.Error())
		return ""
	}
	defer os.Remove(zipPath)

	// Unzip the file
	zipReader, err := zip.OpenReader(zipPath)
	if err != nil {
		fmt.Println("Failed to read zip file that was written to disk: ", err.Error())
		return ""
	}
	defer zipReader.Close()

	if len(zipReader.File) > 1 {
		fmt.Println("Uh Oh, found more than one file in the manifest zip... ignoring all but the first.")
	}

	sqliteName := zipReader.File[0].Name
	zipF, _ := zipReader.File[0].Open()

	sqliteOutput, err := os.OpenFile(sqliteName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening sqlite file to write to disk: ", err.Error())
		return ""
	}
	defer sqliteOutput.Close()
	defer zipF.Close()

	io.Copy(sqliteOutput, zipF)

	// Return the path to the unzipped SQLite DB
	return sqliteName
}

// processManifestDB will process the manifest sqlite database for the specified Locale,
// reading the desired fields out of the manifest and inserting them into the new relational DB.
// The checksum provided is the md5 of the SQLite database file being processed. This should
// be stored when the new table is written to provide caching support next time.
func processWorldContentsManifestDB(locale, checksum, sqlitePath string, itemLimit int) error {

	in, err := GetInputDBConnection(sqlitePath)
	if err != nil {
		fmt.Println("Error opening the input database: ", err.Error())
		return err
	}
	//defer in.Database.Close()
	_, err = GetOutputDBConnection()
	if err != nil {
		fmt.Println("Error opening output database: ", err.Error())
		return err
	}

	// DestinyInventoryItemDefinitions
	err = parseItemDefinitions(in, locale, checksum, itemLimit)

	// DestinyInventoryBucketDefinition
	err = parseBucketDefinitions(in, locale, checksum)

	// Share a single transaction for all activity related tables as they have foreign key constraints
	// that need to be removed/added to import the new data.
	err = output.OpenTransaction()
	if err != nil {
		panic("Failed to open activities transaction: " + err.Error())
	}
	output.RemoveActivityConstraints()
	output.CommitTransaction()

	err = output.OpenTransaction()
	if err != nil {
		panic("Failed to open activities transaction: " + err.Error())
	}
	err = parseActivityModifiers(in, locale, checksum)
	err = parseActivityTypes(in, locale, checksum)
	err = parseActivityModes(in, locale, checksum)
	err = parsePlaces(in, locale, checksum)
	err = parseDestinations(in, locale, checksum)
	err = parseActivities(in, locale, checksum)
	output.CommitTransaction()

	err = output.OpenTransaction()
	if err != nil {
		panic("Failed to open activities transaction: " + err.Error())
	}
	output.AddActivityConstraints()
	err = output.CommitTransaction()
	if err != nil {
		panic("Failed to commit activities transaction: " + err.Error())
	}

	return err
}

func parseItemDefinitions(inputDB *InputDB, locale, checksum string, itemLimit int) error {
	inRows, err := inputDB.GetItemDefinitions()
	if err != nil {
		fmt.Println("Error reading item definitions from sqlite: ", err.Error())
		return err
	}
	defer inRows.Close()

	itemDefs := make([]*ItemDefinition, 0)
	for inRows.Next() {
		row := ManifestRow{}
		inRows.Scan(&row.ID, &row.JSON)

		item := ItemDefinition{}
		json.Unmarshal([]byte(row.JSON), &item)

		itemDefs = append(itemDefs, &item)

		if itemLimit != -1 && len(itemDefs) == itemLimit {
			break
		}
	}

	fmt.Printf("Processed %d item definitions\n", len(itemDefs))

	err = output.DumpNewItemDefintions(locale, checksum, itemDefs)
	if err == nil {
		output.SaveManifestChecksum(locale, checksum)
	}

	return err
}

func parseBucketDefinitions(inputDB *InputDB, locale, checksum string) error {

	bucketRows, err := inputDB.GetBucketDefinitions()
	if err != nil {
		fmt.Println("Error reading item definitions from sqlite: ", err.Error())
		return err
	}
	defer bucketRows.Close()

	bucketDefs := make([]*BucketDefinition, 0)
	for bucketRows.Next() {
		row := ManifestRow{}
		bucketRows.Scan(&row.ID, &row.JSON)

		bucket := BucketDefinition{}
		json.Unmarshal([]byte(row.JSON), &bucket)

		bucketDefs = append(bucketDefs, &bucket)
	}

	fmt.Printf("Processed %d bucket definitions\n", len(bucketDefs))

	return output.DumpNewBucketDefintions(locale, checksum, bucketDefs)
}

func parseActivityModifiers(inputDB *InputDB, locale, checksum string) error {

	modifierRows, err := inputDB.GetActivityModifierDefinitions()
	if err != nil {
		fmt.Println("Error reading item definitions from sqlite: ", err.Error())
		return err
	}
	defer modifierRows.Close()

	modifierDefs := make([]*ActivityModifierDefinition, 0)
	for modifierRows.Next() {
		row := ManifestRow{}
		modifierRows.Scan(&row.ID, &row.JSON)

		modifier := ActivityModifierDefinition{}
		json.Unmarshal([]byte(row.JSON), &modifier)

		modifierDefs = append(modifierDefs, &modifier)
	}

	fmt.Printf("Processed %d activity modifier definitions\n", len(modifierDefs))

	return output.DumpNewActivityModifierDefinitions(locale, checksum, modifierDefs)
}

func parseActivityTypes(inputDB *InputDB, locale, checksum string) error {

	activityTypeRows, err := inputDB.GetActivityTypeDefinitions()
	if err != nil {
		fmt.Println("Error reading activity types from sqlite: ", err.Error())
		return err
	}
	defer activityTypeRows.Close()

	activityTypeDefs := make([]*ActivityTypeDefinition, 0)
	for activityTypeRows.Next() {
		row := ManifestRow{}
		activityTypeRows.Scan(&row.ID, &row.JSON)

		activityType := ActivityTypeDefinition{}
		json.Unmarshal([]byte(row.JSON), &activityType)

		activityTypeDefs = append(activityTypeDefs, &activityType)
	}

	fmt.Printf("Processed %d activity type definitions\n", len(activityTypeDefs))

	return output.DumpNewActivityTypeDefinitions(locale, checksum, activityTypeDefs)
}

func parseActivityModes(inputDB *InputDB, locale, checksum string) error {

	activityModeRows, err := inputDB.GetActivityModeDefinitions()
	if err != nil {
		fmt.Println("Error reading activity mode definitions from sqlite: ", err.Error())
		return err
	}
	defer activityModeRows.Close()

	activityModeDefs := make([]*ActivityModeDefintion, 0)
	for activityModeRows.Next() {
		row := ManifestRow{}
		activityModeRows.Scan(&row.ID, &row.JSON)

		mode := ActivityModeDefintion{}
		json.Unmarshal([]byte(row.JSON), &mode)

		activityModeDefs = append(activityModeDefs, &mode)
	}

	fmt.Printf("Processed %d activity mode definitions\n", len(activityModeDefs))

	return output.DumpNewActivityModeDefinitions(locale, checksum, activityModeDefs)
}

func parseActivities(inputDB *InputDB, locale, checksum string) error {

	activityRows, err := inputDB.GetActivityDefinitions()
	if err != nil {
		fmt.Println("Error reading activity definitions from sqlite: ", err.Error())
		return err
	}
	defer activityRows.Close()

	activityDefs := make([]*ActivityDefinition, 0)
	for activityRows.Next() {
		row := ManifestRow{}
		activityRows.Scan(&row.ID, &row.JSON)

		activity := ActivityDefinition{}
		json.Unmarshal([]byte(row.JSON), &activity)

		activityDefs = append(activityDefs, &activity)
	}

	fmt.Printf("Processed %d activity definitions\n", len(activityDefs))

	return output.DumpNewActivityDefinitions(locale, checksum, activityDefs)
}

func parsePlaces(inputDB *InputDB, locale, checksum string) error {

	placeRows, err := inputDB.GetPlaceDefinitions()
	if err != nil {
		fmt.Println("Error reading place definitions from sqlite: ", err.Error())
		return err
	}
	defer placeRows.Close()

	placeDefs := make([]*PlaceDefinition, 0)
	for placeRows.Next() {
		row := ManifestRow{}
		placeRows.Scan(&row.ID, &row.JSON)

		place := PlaceDefinition{}
		json.Unmarshal([]byte(row.JSON), &place)

		placeDefs = append(placeDefs, &place)
	}

	fmt.Printf("Processed %d place definitions\n", len(placeDefs))

	return output.DumpNewPlaceDefinitions(locale, checksum, placeDefs)
}

func parseDestinations(inputDB *InputDB, locale, checksum string) error {

	destinationRows, err := inputDB.GetDestinationDefinitions()
	if err != nil {
		fmt.Println("Error reading destination defintions from sqlite: ", err.Error())
		return err
	}
	defer destinationRows.Close()

	destinationDefs := make([]*DestinationDefintion, 0)
	for destinationRows.Next() {
		row := ManifestRow{}
		destinationRows.Scan(&row.ID, &row.JSON)

		destination := DestinationDefintion{}
		json.Unmarshal([]byte(row.JSON), &destination)

		destinationDefs = append(destinationDefs, &destination)
	}

	fmt.Printf("Processed %d destination definitions\n", len(destinationDefs))

	return output.DumpNewDestinationDefinitions(locale, checksum, destinationDefs)
}
