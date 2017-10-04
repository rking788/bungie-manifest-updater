package main

import (
	"archive/zip"
	"encoding/json"
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
	DisplayProperties struct {
		Icon        string `json:"icon"`
		Description string `json:"description"`
		ItemName    string `json:"name"`
	} `json:"displayProperties"`
}

type BucketDefinition struct {
	BucketHash        int `json:"hash"`
	DisplayProperties struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	} `json:"displayProperties"`
}

func main() {

	fmt.Printf("Running version=(%s) build on date=(%s)...\n", VERSION, BUILD_DATE)

	client := http.DefaultClient
	req, _ := http.NewRequest("GET", ManifestURL, nil)

	bungieAPIKey := os.Getenv("BUNGIE_API_KEY")
	if bungieAPIKey != "" {
		// Providing an API Key will decrease the chances of the request being throttled
		req.Header.Add("X-Api-Key", bungieAPIKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Failed to make request to Bungie for the manifest spec")
		return
	}
	defer resp.Body.Close()

	manifestSpec := &ManifestSpecResponse{}
	err = json.NewDecoder(resp.Body).Decode(manifestSpec)
	if err != nil {
		fmt.Println("Error parsing manifest spec response: ", err.Error())
		return
	}

	if manifestSpec.ErrorStatus != "Success" || manifestSpec.Message != "Ok" {
		fmt.Println("Error response from Bungie for manifest spec.")
		return
	}

	// manifestChecksums stores the md5sums of the manifest files keyed by the language they
	// are associated with.
	manifestChecksums, err := ReadManifestChecksums()
	if err != nil {
		fmt.Println("Error trying to read manifest checksums")
		return
	}

	for lang, path := range manifestSpec.Response.MobileWorldContentPaths {
		if lang != "en" {
			continue
		}
		fmt.Println("Checking manifest for language: ", lang)
		currentChecksum := manifestChecksums[lang]
		incomingChecksum := checksumFromManifestFilename(path)
		if currentChecksum != "" && currentChecksum == incomingChecksum {
			fmt.Println("Incoming manifest is the same as the current one already stored...skipping!")
			continue
		}

		sqlitePath := downloadMobileWorldContentPath(path, lang)
		defer os.Remove(sqlitePath)

		err = processManifestDB(lang, incomingChecksum, sqlitePath)
	}
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

// parseMobileWorldContentPath will download and unzip the world content database,
// unzip it, and save the sqlite database somewhere on disk. The return value
// is the location on disk where the sqlite database is saved.
func downloadMobileWorldContentPath(resourcePath, language string) string {

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
	zipPath := fmt.Sprintf("world_sql_content_%s.content", language)
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
func processManifestDB(locale, checksum, sqlitePath string) error {

	in, err := GetInputDBConnection(sqlitePath)
	if err != nil {
		fmt.Println("Error opening the input database: ", err.Error())
		return err
	}
	defer in.Database.Close()
	_, err = GetOutputDBConnection()
	if err != nil {
		fmt.Println("Error opening output database: ", err.Error())
		return err
	}

	// DestinyInventoryItemDefinitions
	err = parseItemDefinitions(in, locale, checksum)

	// DestinyInventoryBucketDefinition
	err = parseBucketDefinitions(in, locale, checksum)

	return err
}

func parseItemDefinitions(inputDB *InputDB, locale, checksum string) error {
	inRows, err := input.GetItemDefinitions()
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
