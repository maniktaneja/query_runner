package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func watchFile(filePath string, changeChan chan int, done chan bool) error {
	initialStat, err := os.Stat(filePath)
	if err != nil {
		return err
	}

loop:
	for {

		select {

		case <-time.After(1 * time.Second):
			newstat, err := os.Stat(filePath)
			if err != nil {
				return err
			}

			if newstat.Size() != initialStat.Size() || newstat.ModTime() != initialStat.ModTime() {
				log.Printf(" File changed %v ", filePath)
				initialStat = newstat
				changeChan <- 1
			}
		case <-done:
			break loop

		}
	}

	return nil
}

func TableCreater(filename string, db *sql.DB, done chan bool) {
	defer wg.Done()

	changeChan := make(chan int)
	go watchFile(filename, changeChan, done)
	runCreateQueries(db, filename)

loop:
	for {

		select {
		case <-changeChan:

			fmt.Println("File has been changed")
			runCreateQueries(db, filename)
		case <-done:
			break loop
		}
	}

}

func runCreateQueries(db *sql.DB, filename string) {

	log.Printf(" Executing create table queries ")
	queryLines, err := readLines(filename)
	if err != nil {
		log.Printf(" Unable to read from file %s, Error %v", filename, err)
	}
	executeCreate(db, queryLines)

}

func executeCreate(db *sql.DB, queryLines []string) {

	for _, query := range queryLines {
		_, err := db.Exec(query)
		if err != nil {
			log.Printf("Failed to execute query %v Error %v", query, err)
		}
	}

}

func runInsertQuery(db *sql.DB, queryChannel chan string, wg *sync.WaitGroup) {

	defer wg.Done()

	for {
		select {
		case query, ok := <-queryChannel:
			if !ok {
				log.Printf("Query executer thread exiting ... ")
				return
			}
			res, err := db.Exec(query)
			if err != nil {
				log.Printf("Failed to execute query %v Error %v", query, err)
			} else {
				if ra, err := res.RowsAffected(); err == nil {
					log.Printf("Rows Affected %v", ra)
				}
			}
		}
	}
}

// schedule analyze or delete queries
func queryScheduler(queryFile string, db *sql.DB, doneChan chan bool) {

	defer wg.Done()

	var qwg sync.WaitGroup
	queryChannel := make(chan string, 32)

	for i := 0; i < runtime.NumCPU()*2; i++ {
		qwg.Add(1)
		go runInsertQuery(db, queryChannel, &qwg)
	}

	ok := true
	for ok == true {
		select {
		case <-time.After(1 * time.Minute):
			// execute insert queries
			queryLines, err := readLines(queryFile)
			if err != nil {
				log.Printf(" Unable to read from file %s, Error %v", queryFile, err)
				continue
			}

			for _, query := range queryLines {
				queryChannel <- query
			}

		case <-doneChan:
			ok = false
			close(queryChannel)
		}
	}

	qwg.Wait()
	log.Printf("Query scheduler thread exiting...")
}

func ConnectMemsql(config dbConfig) (*sql.DB, error) {
	netAddr := fmt.Sprintf("tcp(%s:%s)", config.dbHost, config.dbPort)
	dsn := fmt.Sprintf("%s:%s@%s/%s?timeout=30s&strict=true&allowAllFiles=true", config.dbUser, config.dbPass, netAddr, config.dbName)

	var err error
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("Open db error %v", err)
		return nil, fmt.Errorf("Open db error")
	}

	err = db.Ping()
	if err != nil {
		log.Printf("Connection not established Error %v", err)
		db.Close()
		return nil, fmt.Errorf("Ping error")
	}

	return db, nil
}

type dbConfig struct {
	dbHost string
	dbPort string
	dbUser string
	dbPass string
	dbName string
}

var threads = flag.Int("threads", runtime.NumCPU(), "number of threads")
var queryFile = flag.String("queryfile", "query_file.txt", "file containing list of select queries")
var createFile = flag.String("createFile", "create_file.txt", "file containing create table queries")
var dbHost = flag.String("host", "localhost", "d/b hostname")
var dbPort = flag.String("port", "3306", "d/b port")
var dbUser = flag.String("username", "admin", "d/b username")
var dbPass = flag.String("password", "admin", "d/b password")
var dbName = flag.String("name", "badger", "d/b name")

var wg sync.WaitGroup

func main() {

	flag.Parse()

	// set GO_MAXPROCS to the number of threads

	runtime.GOMAXPROCS(*threads)

	// validation
	if *dbHost == "" || *dbPort == "" || *dbUser == "" || *dbPass == "" || *dbName == "" {
		log.Fatalf("Incorrect credentials")
	}

	config := &dbConfig{
		dbHost: *dbHost,
		dbPort: *dbPort,
		dbPass: *dbPass,
		dbUser: *dbUser,
		dbName: *dbName,
	}

	db, err := ConnectMemsql(*config)
	if err != nil {
		log.Fatalf("Unable to connect to sql instance. Error %v", err)
	}

	if _, err := os.Stat(*queryFile); os.IsNotExist(err) {
		log.Fatalf("Query File %v doesn't exist.", *queryFile)
	}

	if _, err := os.Stat(*createFile); os.IsNotExist(err) {
		log.Fatalf("Create File %v doesn't exist.", *createFile)
	}

	doneChan := make(chan bool)
	go TableCreater(*createFile, db, doneChan)
	wg.Add(1)

	go queryScheduler(*queryFile, db, doneChan)
	wg.Add(1)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	close(doneChan)

	wg.Wait()
}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func returnValue(pval *interface{}) interface{} {
	switch v := (*pval).(type) {
	case nil:
		return "NULL"
	case bool:
		if v {
			return true
		} else {
			return false
		}
	case []byte:
		return string(v)
	case time.Time:
		return v.Format("2006-01-02 15:04:05.999")
	default:
		return v
	}
}
