package main

import (
	"bytes"
	"context"
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

const authHdrKey = "x-wwcp-auth"

var (
	key      = flag.String("key", "", "feed id (required)")
	auth     = flag.String("auth", "", "auth key")
	base     = flag.String("base", "https://wwcp540.appspot.com/", "wwcp base URL")
	dest     = flag.String("dest", "", "destination URL")
	pollFreq = flag.Duration("poll-freq", 5*time.Minute,
		"how frequently to check for messages")
	timeout = flag.Duration("timeout", 4*time.Minute+30*time.Second, "poll pass timeout")
	noop    = flag.Bool("noop", false, "if true, just show what we would do")

	parsedBase, parsedDest *url.URL

	errDone = errors.New("done")
)

// A Message is an individual message received at an endpoint on behalf of this client.
type Message struct {
	TID         string
	Created     time.Time   `json:"created"`
	RemoteAddr  string      `json:"remote_addr"`
	QueryString string      `json:"query"`
	Header      http.Header `json:"headers"`
	Body        []byte      `json:"body"`
}

func deliver(ctx context.Context, msg *Message) error {
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
	req = req.WithContext(ctx)
	req.Header = msg.Header
	if req.Header == nil {
		req.Header = http.Header{}
	}
	req.Header.Set("X-Forwarded-For", msg.RemoteAddr)
	if *noop {
		req.Write(os.Stdout)
		os.Stdout.Write([]byte{'\n', '\n'})
		return nil
	}
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
	if *noop {
		log.Printf("Deleting queued item %v", tid)
		return nil
	}
	u := *parsedBase
	u.Path = "/q/rm/" + *key + "/" + tid
	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set(authHdrKey, *auth)
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

func processOne(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	u := *parsedBase
	u.Path = "/q/pull/" + *key
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.Header.Set(authHdrKey, *auth)

	res, err := http.DefaultClient.Do(req)
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
	if err = deliver(ctx, msg); err != nil {
		return err
	}

	return deleteItem(msg.TID)
}

// poll grabs all the available items from the wwcp upstream and
// processes them, returning when there are no more to process.
func poll(ctx context.Context) error {
	for {
		select {
		case <-quitch:
			return nil
		default:
		}

		err := processOne(ctx)
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

var quitch = make(chan struct{})

func main() {
	flag.Parse()

	if *key == "" {
		flag.Usage()
		log.Fatalf("Key is required")
	}

	ctx := context.Background()

	parsedBase = mustParse(*base)
	parsedDest = mustParse(*dest)

	httputil.InitHTTPTracker(true)

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt)
	go func() {
		<-sigch
		log.Printf("Got signal, shutting down.")
		close(quitch)
	}()

	if err := poll(ctx); err != nil {
		log.Printf("Error polling: %v", err)
	}

	tick := time.Tick(*pollFreq)
	for {
		select {
		case <-tick:
			if err := poll(ctx); err != nil {
				log.Printf("Error polling: %v", err)
			}
		case <-quitch:
			return
		}
	}
}
