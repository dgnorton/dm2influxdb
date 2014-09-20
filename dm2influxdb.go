package main

import (
	"flag"
	"fmt"
	"log"
	"os/user"
	"github.com/dgnorton/dmapi"
	influx "github.com/influxdb/influxdb/client"
)

func main() {
	dmuser := flag.String("u", "", "dailymile username")
	flag.Parse()
	if *dmuser == "" {
		flag.Usage()
		return
	}
	osuser, err := user.Current()
	fatalIfErr(err)
	dmdata := fmt.Sprintf("%s/.dailymile_cli/%s/entries.json", osuser.HomeDir, *dmuser)
	entries, err := dmapi.LoadEntries(dmdata)
	fatalIfErr(err)
	cfg := &influx.ClientConfig{Username: "root", Password: "root", Database: "dailymile"}
	client, err := influx.NewClient(cfg)
	fatalIfErr(err)
	dblist, err := client.GetDatabaseList()
	fatalIfErr(err)
	if !dbExists(cfg.Database, dblist) {
		fmt.Printf("Creating database: %s\n", cfg.Database)
		err = client.CreateDatabase(cfg.Database)
		fatalIfErr(err)
	}
	series, err := entries2Series(*dmuser, entries)
	fatalIfErr(err)
	//fmt.Println(series)
	err = client.WriteSeriesWithTimePrecision([]*influx.Series{series}, influx.Second)
	fatalIfErr(err)
}

func entries2Series(dmuser string, entries *dmapi.Entries) (*influx.Series, error) {
	series := &influx.Series{
		Name: dmuser,
		Columns: []string{"time", "distance", "duration", "title"},
		Points: [][]interface{}{},
	}

	for i, entry := range entries.Entries {
		//if len(entries.Entries) > 500 && i < len(entries.Entries) - 500 {
		if i > 300 {
			break // only store the last 500 entries
		}
		tm, err := entry.Time()
		if err != nil {
			return nil, err
		}
		dist := entry.Workout.Distance.Value
		dur := entry.Workout.Duration().Seconds()
		title := entry.Workout.Title
		point := []interface{}{float64(tm.Unix()), dist, dur, title}
		series.Points = append(series.Points, point)
	}
	return series, nil
}

func dbExists(dbname string, dblist []map[string]interface{}) bool {
	for _, db := range dblist {
		if dbname == db["name"] {
			return true
		}
	}
	return false
}
func fatalIfErr(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
