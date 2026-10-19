// Command eventstream_load runs the durable event stream load test's SDK app,
// sender and database check; run.sh drives the full sequence.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/inngest/inngestgo"
)

const (
	eventName  = "loadtest/event"
	appID      = "eventstream-load"
	signingKey = "6c6f6164746573742d7369676e696e672d6b6579"
	eventKey   = "loadtest-event-key"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: eventstream_load app|register|drive [flags]")
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "app":
		err = runApp(os.Args[2:])
	case "register":
		err = register(os.Args[2:])
	case "drive":
		err = drive(os.Args[2:])
	default:
		err = fmt.Errorf("unknown mode %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func newClient(api, appURL string) (inngestgo.Client, error) {
	key, ek, dev := signingKey, eventKey, false
	registerURL := api + "/fn/register"
	c, err := inngestgo.NewClient(inngestgo.ClientOpts{
		AppID:       appID,
		SigningKey:  &key,
		EventKey:    &ek,
		APIBaseURL:  &api,
		RegisterURL: &registerURL,
		Dev:         &dev,
		Logger:      slog.New(slog.DiscardHandler),
	})
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(appURL)
	if err != nil {
		return nil, err
	}
	c.SetURL(u)

	_, err = inngestgo.CreateFunction(c,
		inngestgo.FunctionOpts{ID: "loadtest-noop"},
		inngestgo.EventTrigger(eventName, nil),
		func(context.Context, inngestgo.Input[any]) (any, error) { return nil, nil },
	)
	return c, err
}

// runApp serves the SDK app the server calls back into.
func runApp(args []string) error {
	fs := flag.NewFlagSet("app", flag.ExitOnError)
	listen := fs.String("listen", ":13939", "address to serve the SDK app on")
	api := fs.String("api", "http://localhost:18288", "inngest server URL")
	appURL := fs.String("app-url", "http://host.docker.internal:13939/api/inngest", "app URL as the server reaches it")
	_ = fs.Parse(args)

	c, err := newClient(*api, *appURL)
	if err != nil {
		return err
	}
	// The register mode's PUT is unsigned, as from a deploy script.
	unauthedSync := true
	mux := http.NewServeMux()
	mux.Handle("/api/inngest", c.ServeWithOpts(inngestgo.ServeOpts{EnableUnauthedSync: &unauthedSync}))
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	return srv.ListenAndServe()
}

// register asks the running app to sync itself with the server.
func register(args []string) error {
	fs := flag.NewFlagSet("register", flag.ExitOnError)
	app := fs.String("app", "http://localhost:13939/api/inngest", "local SDK app URL")
	_ = fs.Parse(args)

	req, err := http.NewRequest(http.MethodPut, *app, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("app sync returned %d", resp.StatusCode)
	}
	return nil
}
