package main

import (
	"flag"
	"fmt"
	"github.com/dgnorton/dmapi"
	influx "github.com/influxdb/influxdb/client"
	"log"
	"os/user"
)

func main() {
	// parse command line options
	var dmUser string
	var maxRecords int
	flag.StringVar(&dmUser, "u", "", "dailymile username")
	flag.IntVar(&maxRecords, "m", -1, "max number of records to insert into database")
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
	cfg := &influx.ClientConfig{Username: "root", Password: "root", Database: "dailymile"}
	client, err := influx.NewClient(cfg)
	fatalIfErr(err)
	dbList, err := client.GetDatabaseList()
	fatalIfErr(err)
	if !dbExists(cfg.Database, dbList) {
		fmt.Printf("Creating database: %s\n", cfg.Database)
		err = client.CreateDatabase(cfg.Database)
		fatalIfErr(err)
	} else {
		fmt.Printf("Database already exists: %s\n", cfg.Database)
	}

	// drop existing series for user if it already exists
	_, err = client.Query(fmt.Sprintf("drop series %s.distance", dmUser))
	fatalIfErr(err)
	_, err = client.Query(fmt.Sprintf("drop series %s.duration", dmUser))
	fatalIfErr(err)
	_, err = client.Query(fmt.Sprintf("drop series %s.pace", dmUser))
	fatalIfErr(err)

	series, err := entries2Series(dmUser, entries, maxRecords)
	fatalIfErr(err)
	err = client.WriteSeriesWithTimePrecision(series, influx.Second)
	fatalIfErr(err)
}

func entries2Series(dmUser string, entries *dmapi.Entries, maxRecords int) ([]*influx.Series, error) {
	distanceSeries := &influx.Series{
		Name:    fmt.Sprintf("%s.distance", dmUser),
		Columns: []string{"time", "distance"},
		Points:  [][]interface{}{},
	}

	durationSeries := &influx.Series{
		Name:    fmt.Sprintf("%s.duration", dmUser),
		Columns: []string{"time", "duration"},
		Points:  [][]interface{}{},
	}

	paceSeries := &influx.Series{
		Name:    fmt.Sprintf("%s.pace", dmUser),
		Columns: []string{"time", "pace", "paceStr"},
		Points:  [][]interface{}{},
	}

	recordCnt := 0
	for _, entry := range entries.Entries {
		if entry.Workout.Type == "" {
			continue
		}
		if maxRecords > -1 && recordCnt >= maxRecords {
			break
		}
		recordCnt++
		tm, err := entry.Time()
		if err != nil {
			return nil, err
		}
		distance := entry.Workout.Distance.Value
		point := []interface{}{float64(tm.Unix()), distance}
		distanceSeries.Points = append(distanceSeries.Points, point)

		duration := entry.Workout.Duration().Seconds() / 60
		point = []interface{}{float64(tm.Unix()), duration}
		durationSeries.Points = append(durationSeries.Points, point)

		if distance > 0 && duration > 0 {
			p, err := entry.Workout.Pace()
			if err != nil {
				fmt.Println("pace error")
				continue
			}
			paceStr := dmapi.DurationStr(p)
			pace := p.Seconds() / 60

			point = []interface{}{float64(tm.Unix()), pace, paceStr}
			paceSeries.Points = append(paceSeries.Points, point)
		}
	}
	return []*influx.Series{distanceSeries, durationSeries, paceSeries}, nil
}

func dbExists(dbname string, dbList []map[string]interface{}) bool {
	for _, db := range dbList {
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
