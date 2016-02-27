package wwcp

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"text/template"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
	"google.golang.org/appengine/taskqueue"
	"google.golang.org/appengine/user"
)

const (
	maxBytes   = 2 << 20
	authHdrKey = "x-wwcp-auth"
)

func init() {
	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/feeds/", handleListFeeds)
	http.HandleFunc("/feeds/new", handleNewFeed)
	http.HandleFunc("/feeds/rekey/", handleRekey)

	http.HandleFunc("/q/push/", handlePush)
	http.HandleFunc("/q/pull/", handlePull)
	http.HandleFunc("/q/rm/", handleComplete)
}

// A Feed is an endpoint that collects posted Messages.
type Feed struct {
	Owner string
	Name  string
	Auth  string

	Key *datastore.Key `datastore:"-"`
}

// A Message is posted to an endpoint to be replayed later by a client.
type Message struct {
	Created     time.Time   `json:"created"`
	RemoteAddr  string      `json:"remote_addr"`
	QueryString string      `json:"query"`
	Header      http.Header `json:"header"`
	Body        []byte      `json:"body"`
}

var templates = template.Must(template.New("").ParseGlob("templates/*.html"))

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if err := templates.ExecuteTemplate(w, "index.html", nil); err != nil {
		reportError(appengine.NewContext(r), w, err)
	}
}

func reportError(c context.Context, w http.ResponseWriter, err error) {
	log.Warningf(c, "Error: %v", err)
	http.Error(w, "Error processing your request", 500)
}

func handleListFeeds(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	q := datastore.NewQuery("Feed").Filter("Owner = ", user.Current(c).Email)
	feeds := []Feed{}
	keys, err := q.GetAll(c, &feeds)
	if err != nil {
		reportError(c, w, err)
		return
	}

	for i := range feeds {
		feeds[i].Key = keys[i]
	}

	err = templates.ExecuteTemplate(w, "feeds.html", struct {
		Feeds    []Feed
		Hostname string
	}{feeds, r.Host})
	if err != nil {
		reportError(c, w, err)
	}
}

func genAuth() string {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		panic("can't get randomness")
	}
	return hex.EncodeToString(b)
}

func handleNewFeed(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	feed := &Feed{
		Owner: user.Current(c).Email,
		Name:  r.FormValue("name"),
		Auth:  genAuth(),
	}

	_, err := datastore.Put(c, datastore.NewIncompleteKey(c, "Feed", nil), feed)
	if err != nil {
		reportError(c, w, err)
		return
	}

	http.Redirect(w, r, "/feeds/", http.StatusSeeOther)
}

func handleRekey(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	if r.Method != "POST" {
		http.Error(w, r.Method+" not allowed here", 400)
		return
	}
	kstr := r.URL.Path[13:]

	k, err := datastore.DecodeKey(kstr)
	if err != nil {
		log.Warningf(c, "Failed to decode key %q: %v", kstr, err)
		http.Error(w, "not found", 404)
		return
	}

	feed := &Feed{}
	err = datastore.Get(c, k, feed)
	if err != nil {
		log.Warningf(c, "Failed to fetch feed %q: %v", kstr, err)
		http.Error(w, "not found", 404)
		return
	}
	if feed.Owner != user.Current(c).Email {
		log.Warningf(c, "%v belongs to %v, not %v", kstr, feed.Owner, user.Current(c).Email)
		http.Error(w, "not found", 404)
		return
	}
	feed.Auth = genAuth()
	_, err = datastore.Put(c, k, feed)
	if err != nil {
		reportError(c, w, err)
		return
	}
	feedCache.Set(kstr, feed)

	http.Redirect(w, r, "/feeds/", http.StatusSeeOther)
}

type cachedFeed struct {
	feed *Feed
	ttl  time.Time
}

func (c *cachedFeed) Feed() *Feed {
	if c == nil {
		return nil
	}
	return c.feed
}

type feedCacheT struct {
	m  map[string]*cachedFeed
	mu sync.Mutex
}

func (f *feedCacheT) Get(name string) (*Feed, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cf, ok := f.m[name]
	if cf != nil && cf.ttl.Before(time.Now()) {
		delete(f.m, name)
		cf.feed = nil
		ok = false
	}
	return cf.Feed(), ok
}

func (f *feedCacheT) Set(name string, feed *Feed) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.m[name] = &cachedFeed{feed: feed, ttl: time.Now().Add(time.Hour)}
}

var (
	feedCache = &feedCacheT{m: map[string]*cachedFeed{}}
	errNoFeed = errors.New("no such feed")
)

func getFeed(c context.Context, kstr string) (*Feed, error) {
	feed, ok := feedCache.Get(kstr)
	if ok {
		if feed == nil {
			return nil, errNoFeed
		}
		return feed, nil
	}

	k, err := datastore.DecodeKey(kstr)
	if err != nil {
		return nil, err
	}

	feed = &Feed{}
	err = datastore.Get(c, k, feed)
	feed.Key = k
	if err == nil {
		feedCache.Set(kstr, feed)
	} else {
		feedCache.Set(kstr, nil)
	}
	return feed, err
}

func compress(c context.Context, in []byte) []byte {
	buf := &bytes.Buffer{}
	gz := gzip.NewWriter(buf)
	_, err := gz.Write(in)
	if err != nil {
		log.Warningf(c, "Error compressing: %v", err)
		return in
	}
	err = gz.Close()
	if err != nil {
		log.Warningf(c, "Error closing compressed stream: %v", err)
		return in
	}
	log.Infof(c, "Compressed from %v to %v", len(in), buf.Len())
	return buf.Bytes()
}

func uncompress(c context.Context, in []byte) []byte {
	gz, err := gzip.NewReader(bytes.NewReader(in))
	if err == gzip.ErrHeader {
		log.Infof(c, "Data was not compressed")
		return in
	}
	if err != nil {
		log.Warningf(c, "Error opening gzip data: %v", err)
		return in
	}
	buf := &bytes.Buffer{}
	_, err = io.Copy(buf, gz)
	if err != nil {
		log.Warningf(c, "Error reading gzip data: %v", err)
		return in
	}
	return buf.Bytes()
}

func handlePush(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	kstr := r.URL.Path[8:]
	_, err := getFeed(c, kstr)
	if err != nil {
		reportError(c, w, err)
		return
	}

	body, err := ioutil.ReadAll(http.MaxBytesReader(w, r.Body, maxBytes))
	if err != nil {
		reportError(c, w, err)
		return
	}

	msg := &Message{
		Created:     time.Now().UTC(),
		RemoteAddr:  r.RemoteAddr,
		Header:      r.Header,
		QueryString: r.URL.Query().Encode(),
		Body:        body,
	}

	buf := &bytes.Buffer{}
	genc := gob.NewEncoder(buf)
	if err := genc.Encode(msg); err != nil {
		reportError(c, w, err)
		return
	}

	if _, err := taskqueue.Add(c, &taskqueue.Task{
		Method:  "PULL",
		Payload: compress(c, buf.Bytes()),
		Tag:     kstr,
	}, "todo"); err != nil {
		reportError(c, w, err)
		return
	}

	memcache.Delete(c, kstr)

	w.WriteHeader(201)
}

func checkAuth(c context.Context, feed *Feed, r *http.Request) bool {
	return r.Header.Get(authHdrKey) == feed.Auth
}

func handlePull(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	kstr := r.URL.Path[8:]
	if kstr == "" {
		http.Error(w, "No key specified", 400)
		return
	}

	if _, err := memcache.Get(c, kstr); err == nil {
		log.Debugf(c, "No items (cached)")
		w.WriteHeader(204)
		return
	}

	feed, err := getFeed(c, kstr)
	if err != nil {
		reportError(c, w, err)
		return
	}
	if !checkAuth(c, feed, r) {
		http.Error(w, "not found", 404)
		return
	}

	tasks, err := taskqueue.LeaseByTag(c, 1, "todo", 30, kstr)
	if err != nil {
		reportError(c, w, err)
		return
	}
	if len(tasks) != 1 {
		log.Infof(c, "No tasks found")
		memcache.Set(c, &memcache.Item{
			Key:        kstr,
			Value:      []byte{},
			Expiration: time.Hour,
		})
		w.WriteHeader(204)
		return
	}

	task := tasks[0]

	buf := bytes.NewReader(uncompress(c, task.Payload))
	gdec := gob.NewDecoder(buf)
	msg := &Message{}
	if err := gdec.Decode(msg); err != nil {
		reportError(c, w, err)
		return
	}

	j := json.NewEncoder(w)
	err = j.Encode(map[string]interface{}{
		"tid":         task.Name,
		"headers":     msg.Header,
		"created":     msg.Created,
		"query":       msg.QueryString,
		"remote_addr": msg.RemoteAddr,
		"body":        msg.Body,
	})
	if err != nil {
		reportError(c, w, err)
		return
	}
}

func handleComplete(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	parts := strings.SplitN(r.URL.Path[6:], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "you're doing it wrong", 400)
		return
	}
	feed, err := getFeed(c, parts[0])
	if err != nil {
		reportError(c, w, err)
		return
	}
	if !checkAuth(c, feed, r) {
		http.Error(w, "not found", 404)
		return
	}

	if err := taskqueue.Delete(c, &taskqueue.Task{Name: parts[1]}, "todo"); err != nil {
		reportError(c, w, err)
		return
	}

	w.WriteHeader(204)
}
