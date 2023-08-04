package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	karaokeDB              = "karaoke-db"
	karaokeFilePath string = "./data/karafuncatalog.csv"
	mongoTimeout           = 30 * time.Second
	mongoURI               = "mongodb://localhost:27017"
	songsCollection        = "songs"
)

var (
	songsIndices = []mongo.IndexModel{
		{
			Keys: bson.D{primitive.E{
				Key:   "id",
				Value: 1,
			}},
			Options: &options.IndexOptions{
				Unique: &unique,
			},
		},
		{
			Keys: bson.D{
				primitive.E{
					Key:   "title",
					Value: 1,
				},
				primitive.E{
					Key:   "artist",
					Value: 1,
				},
				primitive.E{
					Key:   "year",
					Value: 1,
				},
			},
			Options: &options.IndexOptions{
				Unique: &unique,
			},
		},
		{
			Keys: bson.D{
				primitive.E{
					Key:   "title",
					Value: 1,
				},
			},
		},
		{
			Keys: bson.D{
				primitive.E{
					Key:   "artist",
					Value: 1,
				},
			},
		},
	}
	songsSchema bson.M = bson.M{
		"bsonType": "object",
		"required": []string{"id", "title", "artist"},
		"properties": bson.M{
			"id": bson.M{
				"bsonType":    "int",
				"description": "the unique identifier for a song in karafun catalog",
			},
			"title": bson.M{
				"bsonType":    "string",
				"description": "the title of the song",
			},
			"artist": bson.M{
				"bsonType":    "string",
				"description": "the artist of the song",
			},
			"year": bson.M{
				"bsonType":    "int",
				"description": "the year the song was released",
			},
			"duo": bson.M{
				"bsonType":    "bool",
				"description": "whether the song is a duet",
			},
			"explicit": bson.M{
				"bsonType":    "bool",
				"description": "whether the song is explicit",
			},
			"dateAdded": bson.M{
				"bsonType":    "date",
				"description": "the date the song was added to the catalog",
			},
			"styles": bson.M{
				"bsonType":    "array",
				"description": "the styles of the song",
				"items": bson.M{
					"bsonType": "string",
				},
			},
			"languages": bson.M{
				"bsonType":    "array",
				"description": "the languages of the song",
				"items": bson.M{
					"bsonType": "string",
				},
			},
		},
	}
	unique bool = true
)

type Song struct {
	ID        int       `bson:"id"`        // 0
	Title     string    `bson:"title"`     // 1
	Artist    string    `bson:"artist"`    // 2
	Year      int       `bson:"year"`      // 3
	Duo       bool      `bson:"duo"`       // 4
	Explicit  bool      `bson:"explicit"`  // 5
	DateAdded time.Time `bson:"dateAdded"` // 6
	Styles    []string  `bson:"styles"`    // 7
	Languages []string  `bson:"languages"` // 8
}

func ensureSongsCollection(ctx context.Context, c *mongo.Client) {
	// retrieve collections from db
	clcts, err := c.Database(karaokeDB).ListCollectionNames(ctx, bson.D{{}})
	if err != nil {
		fmt.Printf("Error listing collections: %v", err)
		panic(err)
	}

	// check if collection exists
	for _, clct := range clcts {
		if clct == "songs" {
			// make sure the schema is up-to-date
			ensureSongsSchema(ctx, c)
			return
		}
	}

	// create the collection with schema
	if err := c.Database(karaokeDB).
		CreateCollection(
			ctx,
			"songs",
			options.CreateCollection().SetValidator(bson.M{
				"$jsonSchema": songsSchema,
			})); err != nil {
		fmt.Printf("Error creating collection: %v", err)
		panic(err)
	}
}

func ensureSongsIndices(ctx context.Context, c *mongo.Client) {
	// create a map with index names
	sim := make(map[string]mongo.IndexModel, len(songsIndices))

	// iterate each index for the collection
	for _, si := range songsIndices {
		// check to see if name is already defined
		if si.Options != nil && si.Options.Name != nil {
			sim[*si.Options.Name] = si
			continue
		}

		// name does not already exist, figure out what it should be
		fields := si.Keys.(bson.D)
		var in strings.Builder
		for i, field := range fields {
			if i > 0 {
				fmt.Fprint(&in, "_")
			}

			fmt.Fprintf(&in, "%s_%d", field.Key, field.Value)
		}

		// put the index name in the map
		sim[in.String()] = si
	}

	// retrieve existing indices from db
	mi := c.Database(karaokeDB).Collection(songsCollection).Indexes()
	cur, err := mi.List(ctx)
	if err != nil {
		fmt.Printf("Error retrieving existing indices: %v", err)
		panic(err)
	}

	var eidx []bson.M
	if err = cur.All(ctx, &eidx); err != nil {
		fmt.Printf("Error reading existing indices: %v", err)
		panic(err)
	}

	// remove any extraneous indices
	for _, idx := range eidx {
		if n, ok := idx["name"].(string); ok {
			// skip builtin ID index
			if n == "_id_" {
				continue
			}

			// check to see if an existing index should no longer exist
			if _, ok := sim[n]; !ok {
				if _, err := mi.DropOne(ctx, n); err != nil {
					fmt.Printf("Error dropping index (%s): %v", n, err)
					panic(err)
				}
			}
		}
	}

	// create any missing indices
	if _, err := mi.CreateMany(ctx, songsIndices); err != nil {
		fmt.Printf("Error creating indices: %v", err)
		panic(err)
	}
}

func ensureSongsSchema(ctx context.Context, c *mongo.Client) {
	cmd := bson.D{
		primitive.E{
			Key:   "collMod",
			Value: songsCollection,
		},
		primitive.E{
			Key: "validator",
			Value: bson.D{primitive.E{
				Key:   "$jsonSchema",
				Value: songsSchema,
			}},
		},
		primitive.E{
			Key:   "validationLevel",
			Value: "moderate",
		},
	}

	if err := c.Database(karaokeDB).RunCommand(ctx, cmd).Err(); err != nil {
		fmt.Printf("Error updating schema: %v", err)
		panic(err)
	}
}

func readSongs() []Song {
	// read the CSV cf
	cf, err := os.Open(karaokeFilePath)
	if err != nil {
		fmt.Printf("Error opening file (%s): %v", karaokeFilePath, err)
		panic(err)
	}
	defer cf.Close()

	// create a new CSV reader
	rdr := csv.NewReader(cf)
	rdr.Comma = ';'

	// parse the CSV
	rcrds, err := rdr.ReadAll()
	if err != nil {
		fmt.Printf("Error parsing CSV file (%s): %v", karaokeFilePath, err)
		panic(err)
	}

	// create a slice of songs
	sngs := make([]Song, 0, len(rcrds)-1)
	for i, rcrd := range rcrds {
		if i == 0 {
			continue
		}

		sng := Song{
			Title:  rcrd[1],
			Artist: rcrd[2],
		}

		// parse the id
		if id, err := strconv.Atoi(rcrd[0]); err == nil {
			sng.ID = id
		}

		// parse the year
		if yr, err := strconv.Atoi(rcrd[3]); err == nil {
			sng.Year = yr
		}

		// parse the duo
		if duo, err := strconv.ParseBool(rcrd[4]); err == nil {
			sng.Duo = duo
		}

		// parse the explicit
		if expl, err := strconv.ParseBool(rcrd[5]); err == nil {
			sng.Explicit = expl
		}

		// parse the date added
		if da, err := time.Parse("2006-01-02", rcrd[6]); err == nil {
			sng.DateAdded = da
		}

		// parse the styles
		sng.Styles = strings.Split(rcrd[7], ",")

		// parse the languages
		sng.Languages = strings.Split(rcrd[8], ",")

		// add the song
		sngs = append(sngs, sng)
	}

	return sngs
}

func main() {
	// read the songs
	sngs := readSongs()

	// connect to the database
	ctx, cancel := context.WithTimeout(context.Background(), mongoTimeout)
	defer cancel()

	c, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		fmt.Printf("Error connecting to MongoDB (%s): %v", mongoURI, err)
		panic(err)
	}

	// ensure the collection is created with indices as appropriate
	ensureSongsCollection(ctx, c)
	ensureSongsIndices(ctx, c)

	// insert all of the songs into MongoDB
	clctn := c.Database(karaokeDB).Collection(songsCollection)
	n := 0
	for _, sng := range sngs {
		fmt.Printf("Upserting song (%d): \"%s\" by %s\n", sng.ID, sng.Title, sng.Artist)

		err := clctn.FindOneAndUpdate(
			ctx,
			bson.M{"id": sng.ID},
			bson.M{"$set": sng},
			options.FindOneAndUpdate().SetUpsert(true)).Err()

		// track newly inserted songs
		if err == mongo.ErrNoDocuments {
			n++
			continue
		}

		if err != nil {
			fmt.Printf("Error inserting song (%d): %v", sng.ID, err)
			panic(err)
		}
	}

	fmt.Printf("Import complete: inserted %d songs and updated %d songs!\n", n, (len(sngs) - n))
}
