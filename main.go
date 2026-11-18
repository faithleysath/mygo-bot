package main

import (
	"log"
	"net/http"
	"time"
)

func main() {
	db, err := openStore("mygo-bot.sqlite")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	server := &http.Server{Addr: "127.0.0.1:8080", Handler: newAgent(db).routes(), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("listening on http://%s", server.Addr)
	log.Fatal(server.ListenAndServe())
}
