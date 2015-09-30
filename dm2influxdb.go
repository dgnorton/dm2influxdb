package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os/user"
	"strings"

	"github.com/dgnorton/dmapi"
	influx "github.com/influxdb/influxdb/client"
)

func main() {
	// parse command line options
	var database string
	var dmUser string
	var maxRecords int
	var workoutTypes string
	flag.StringVar(&database, "d", "dailymile", "database name")
	flag.StringVar(&dmUser, "u", "", "dailymile username")
	flag.IntVar(&maxRecords, "m", -1, "max number of records to insert into database")
	flag.StringVar(&workoutTypes, "t", "", "dailymile workout types (comma delimited string)")
	flag.Parse()
	if dmUser == "" {
		flag.Usage()
		return
	}
	osUser, err := user.Current()
	fatalIfErr(err)

	// load local dailymile data for user specified on command line
	dmData := fmt.Sprintf("%s/.dailymile_cli/%s/entries.json", osUser.HomeDir, dmUser)
	entries, err := dmapi.LoadEntries(dmData)
	fatalIfErr(err)

	// create dailymile database in InfluxDB, if it doesn't exist
	url, _ := url.Parse("http://localhost:8086")
	cfg := influx.Config{URL: *url}
	client, err := influx.NewClient(cfg)
	fatalIfErr(err)
	fatalIfErr(createDBIfNotExists(client, database))

	points, err := entries2Points(dmUser, entries, maxRecords, workoutTypes)
	fatalIfErr(err)
	bps := influx.BatchPoints{
		Points:          points,
		Database:        database,
		RetentionPolicy: "default",
	}

	_, err = client.Write(bps)
	fatalIfErr(err)
}

// queryDB convenience function to query the database
func queryDB(c *influx.Client, cmd, db string) (res []influx.Result, err error) {
	q := influx.Query{
		Command:  cmd,
		Database: db,
	}
	if response, err := c.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	}
	return
}

func createDBIfNotExists(c *influx.Client, name string) error {
	_, err := queryDB(c, "CREATE DATABASE IF NOT EXISTS "+name, "")
	return err
}

func entries2Points(dmUser string, entries *dmapi.Entries, maxRecords int, workoutTypes string) ([]influx.Point, error) {
	recordCnt := 0
	points := []influx.Point{}
	for _, entry := range entries.Entries {
		if entry.Workout.Type == "" {
			continue // skip non-workout entries
		}
		if workoutTypes != "" && !strings.Contains(workoutTypes, entry.Workout.Type) {
			continue // skip entry because it's not a type we want
		}
		if maxRecords > -1 && recordCnt >= maxRecords {
			break // we've reached the maxRecords limit
		}
		recordCnt++
		tm, err := entry.Time()
		if err != nil {
			return nil, err
		}
		distance := entry.Workout.Distance.Value
		duration := entry.Workout.Duration().Seconds() / 60

		pace := 0.0
		paceStr := ""
		if distance > 0 && duration > 0 {
			p, err := entry.Workout.Pace()
			if err != nil {
				fmt.Println("pace error")
				continue
			}
			paceStr = dmapi.DurationStr(p)
			pace = p.Seconds() / 60
		}
		point := influx.Point{
			Measurement: "workout",
			Tags: map[string]string{
				"user": dmUser,
				"type": entry.Workout.Type,
			},
			Fields: map[string]interface{}{
				"distance": distance,
				"duration": duration,
				"pace":     pace,
				"pace_str": paceStr,
			},
			Time:      tm,
			Precision: "s",
		}
		points = append(points, point)
	}
	return points, nil
}

func fatalIfErr(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
