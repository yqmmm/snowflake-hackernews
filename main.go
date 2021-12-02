package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)

func fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func Schema(db *sql.DB) {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS items (id INTEGER, value TEXT, PRIMARY KEY (id))")
	fatal(err)
}

func MaxItem() int {
	resp, err := http.DefaultClient.Get("https://hacker-news.firebaseio.com/v0/maxitem.json")
	fatal(err)

	body, err := ioutil.ReadAll(resp.Body)
	fatal(err)

	max, err := strconv.Atoi(string(body))
	fatal(err)

	return max
}

func GetItem(client *http.Client, id int) (string, error) {
	resp, err := client.Get(fmt.Sprintf("https://hacker-news.firebaseio.com/v0/item/%v.json", id))
	if err != nil {
		return "", err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func main() {
	maxItem := MaxItem()

	db, err := sql.Open("sqlite3", "file:test.db?_journal_mode=OFF&_synchronous=OFF&_cache_size=100000&_locking_mode=EXCLUSIVE")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	Schema(db)

	type Result struct {
		Id    int
		Value string
	}
	inputChan := make(chan int, 1000)
	resultChan := make(chan Result)

	// Input
	go func() {
		for i := 1; i <= maxItem; i++ {
			inputChan <- i
		}
	}()

	// Worker
	for i := 0; i < 512; i++ {
		go func() {
			client := http.Client{}
			for id := range inputChan {
			start:
				if id%1000 == 0 {
					log.Printf("Doing item %v", id)
				}
				item, err := GetItem(&client, id)
				if err != nil {
					log.Printf("Retry item %v", id)
					goto start
				}
				resultChan <- Result{
					Id:    id,
					Value: item,
				}
			}
		}()
	}

	for i := 0; i < maxItem; i++ {
		result := <-resultChan
		db.Exec("INSERT INTO items VALUES (?, ?)", result.Id, result.Value)
	}

	close(inputChan)
}
