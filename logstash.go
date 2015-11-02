package logstash

import (
	"encoding/json"
	"errors"
	"log"
	"net"

	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	conn  net.Conn
	route *router.Route
}

// NewLogstashAdapter creates a LogstashAdapter with UDP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("udp"))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	return &LogstashAdapter{
		route: route,
		conn:  conn,
	}, nil
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {
	for m := range logstream {
		msg := LogstashMessage{
			Message:  m.Data,
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
			Fields:   a.route.Options["fields"],
		}
		js, err := json.Marshal(&msg)
		if err != nil {
			log.Println("logstash:", err)
			continue
		}
		_, err = a.conn.Write(js)
		if err != nil {
			log.Println("logstash:", err)
			continue
		}
	}
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message  string `json:"message"`
	Name     string `json:"docker.name"`
	ID       string `json:"docker.id"`
	Image    string `json:"docker.image"`
	Hostname string `json:"docker.hostname"`
	Fields   string `json:"-"`
}

func UnmarshalObjectString(jsonString string) map[string]interface{} {
	var jsonObj map[string]interface{}

	if jsonString == "" {
		return nil
	}

	b := []byte (jsonString)
	if err := json.Unmarshal(b, &jsonObj); err != nil {
		return nil
	}
	return jsonObj
}

// Custom JSON Marshaller to handle embedded JSON data
func (m *LogstashMessage) MarshalJSON() ([]byte, error) {
	fields := UnmarshalObjectString(m.Fields)
	if fields != nil {
		fields["message"] = m.Message
		fields["docker.name"] = m.Name
		fields["docker.id"] = m.ID
		fields["docker.image"] = m.Image
		fields["docker.hostname"] = m.Hostname
		return json.Marshal(fields)
	}

	// Create a new struct so we don't call this same function again
	type Alias LogstashMessage
	return json.Marshal(&struct {
		*Alias
	}{
		Alias: (*Alias)(m),
	})
}
