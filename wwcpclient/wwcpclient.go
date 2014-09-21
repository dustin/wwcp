package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/dustin/httputil"
)

var (
	key      = flag.String("key", "", "feed id (required)")
	base     = flag.String("base", "https://wwcp540.appspot.com/", "wwcp base URL")
	dest     = flag.String("dest", "", "destination URL")
	pollFreq = flag.Duration("poll-freq", 5*time.Minute,
		"how frequently to check for messages")

	parsedBase, parsedDest *url.URL

	errDone = errors.New("done")
)

type Message struct {
	TID         string
	Created     time.Time   `json:"created"`
	RemoteAddr  string      `json:"remote_addr"`
	QueryString string      `json:"query"`
	Header      http.Header `json:"headers"`
	Body        []byte      `json:"body"`
}

func deliver(msg *Message) error {
	u := *parsedDest
	q, err := url.ParseQuery(msg.QueryString)
	if err == nil {
		vals := u.Query()
		for k, v := range q {
			vals[k] = v
		}
		u.RawQuery = vals.Encode()
	}
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(msg.Body))
	if err != nil {
		return err
	}
	req.Header = msg.Header
	if req.Header == nil {
		req.Header = http.Header{}
	}
	req.Header.Set("X-Forwarded-For", msg.RemoteAddr)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 200 && res.StatusCode < 300 {
		return nil
	}
	return httputil.HTTPError(res)
}

func deleteItem(tid string) error {
	u := *parsedBase
	u.Path = "/q/rm/" + *key + "/" + tid
	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == 204 {
		return nil
	}
	return httputil.HTTPError(res)
}

func processOne() error {
	u := *parsedBase
	u.Path = "/q/pull/" + *key
	res, err := http.Get(u.String())
	if err != nil {
		return err
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case 204:
		return errDone
	case 200:
		// ok
	default:
		return httputil.HTTPError(res)
	}

	msg := &Message{}
	if err = json.NewDecoder(res.Body).Decode(msg); err != nil {
		return err
	}

	log.Printf("Got message %v: query=%q", msg.TID, msg.QueryString)
	if err = deliver(msg); err != nil {
		return err
	}

	return deleteItem(msg.TID)
}

func poll() error {
	for {
		err := processOne()
		switch err {
		case nil:
		case errDone:
			log.Printf("Done!")
			return nil
		default:
			return err
		}
	}
}

func mustParse(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		log.Fatalf("Error parsing base URL: %v", err)
	}
	return u
}

func main() {
	flag.Parse()

	if *key == "" {
		flag.Usage()
		log.Fatalf("Key is required")
	}

	parsedBase = mustParse(*base)
	parsedDest = mustParse(*dest)

	httputil.InitHTTPTracker(true)

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt)

	if err := poll(); err != nil {
		log.Printf("Error polling: %v", err)
	}

	tick := time.Tick(*pollFreq)
	for {
		select {
		case <-tick:
			if err := poll(); err != nil {
				log.Printf("Error polling: %v", err)
			}
		case <-sigch:
			log.Printf("Got signal. Shutting down")
			return
		}
	}
}
