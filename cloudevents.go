package cloudevents

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

var verbose = 2

func Debug(level int, format string, args ...interface{}) {
	if verbose >= level {
		if verbose > 0 {
			// Debug(0) is the only one that doesn't get special formatting
			t := time.Now().Format("2006/01/02 15:04:05")
			format = t + ": " + format + "\n"
		}
		fmt.Fprintf(os.Stderr, format, args...)
	}
}

type Event struct {
	SpecVersion     string            `json:"specversion,omitempty"`
	Type            string            `json:"type,omitempty"`
	Source          string            `json:"source,omitempty"`
	Subject         string            `json:"subject,omitempty"`
	ID              string            `json:"id,omitempty"`
	Time            string            `json:"time,omitempty"`
	SchemaURL       string            `json:"schemaurl,omitempty"`
	DataContentType string            `json:"datacontenttype,omitempty"`
	Extensions      map[string]string `json:"extensions,omitempty"`
	Data            json.RawMessage   `json:"data,omitempty"`
	DataObject      interface{}       `json:"-"`
}

func FromHTTPRequest(r *http.Request) (*Event, bool, error) {
	var err error
	body := []byte{}
	event := Event{}
	isBinary := false

	if r.Body != nil {
		if body, err = ioutil.ReadAll(r.Body); err != nil {
			return nil, false, err
		}
	}

	ct := r.Header.Get("Content-Type")
	Debug(3, "Content-Type: %q", ct)

	if strings.HasPrefix(ct, "application/cloudevents+json") {
		ceMap := map[string][]byte{}
		if err = json.Unmarshal(body, &ceMap); err != nil {
			return nil, false, err
		}
		for k, v := range ceMap {
			// k = strings.ToLower(k)
			value := ""
			json.Unmarshal(v, &value)

			Debug(3, "CE.%s: %v", k, value)

			switch k {
			case "specversion":
				event.SpecVersion = string(value)
			case "datacontenttype":
				event.DataContentType = string(value)
			case "type":
				event.Type = string(value)
			case "source":
				event.Source = string(value)
			case "subject":
				event.Subject = string(value)
			case "id":
				event.ID = string(value)
			case "time":
				event.Time = string(value)
			case "schemaurl":
				event.SchemaURL = string(value)
			case "data":
				event.Data = v
			default:
				if event.Extensions == nil {
					event.Extensions = map[string]string{}
				}
				event.Extensions[k] = string(v)
			}
		}
	} else {
		isBinary = true
		for k, v := range r.Header {
			k = strings.ToLower(k)
			if !strings.HasPrefix(k, "ce-") {
				continue
			}
			Debug(3, "HTTP.Header %s: %v", k, v)

			k = k[3:]

			switch k {
			case "datacontenttype":
				event.DataContentType = v[0]
			case "specversion":
				event.SpecVersion = v[0]
			case "type":
				event.Type = v[0]
			case "source":
				event.Source = v[0]
			case "subject":
				event.Subject = v[0]
			case "id":
				event.ID = v[0]
			case "time":
				event.Time = v[0]
			case "schemaurl":
				event.SchemaURL = v[0]
			default:
				if event.Extensions == nil {
					event.Extensions = map[string]string{}
				}
				event.Extensions[k] = v[0]
			}
		}
		Debug(3, "Body: %v", string(body))
		event.Data = body
	}

	return &event, isBinary, nil
}

func (event *Event) Marshal() ([]byte, error) {
	resMap := map[string]interface{}{}

	if event.SpecVersion != "" {
		resMap["specversion"] = event.SpecVersion
	}
	if event.Type != "" {
		resMap["type"] = event.Type
	}
	if event.Source != "" {
		resMap["source"] = event.Source
	}
	if event.Subject != "" {
		resMap["subject"] = event.Subject
	}
	if event.ID != "" {
		resMap["id"] = event.ID
	}
	if event.Time != "" {
		resMap["time"] = event.Time
	}
	if event.SchemaURL != "" {
		resMap["schemaurl"] = event.SchemaURL
	}
	if event.DataContentType != "" {
		resMap["datacontenttype"] = event.DataContentType
	}
	if event.Data != nil {
		resMap["data"] = event.Data
	} else if event.DataObject != nil {
		dataStr, err := json.Marshal(event.DataObject)
		if err != nil {
			return nil, err
		}
		resMap["data"] = dataStr
	}
	if len(event.Extensions) != 0 {
		for k, v := range event.Extensions {
			resMap[strings.ToLower(k)] = v
		}
	}
	return json.MarshalIndent(resMap, "", "  ")
}

func (event *Event) ToHTTPHeaders(header http.Header) {
	AddHeader(header, "ce-specversion", event.SpecVersion)
	AddHeader(header, "ce-type", event.Type)
	AddHeader(header, "ce-source", event.Source)
	AddHeader(header, "ce-subject", event.Subject)
	AddHeader(header, "ce-id", event.ID)
	AddHeader(header, "ce-time", event.Time)
	AddHeader(header, "ce-datacontenttype", event.DataContentType)
	AddHeader(header, "ce-schemaurl", event.SchemaURL)

	for k, v := range event.Extensions {
		value := []byte{}
		json.Unmarshal([]byte(v), &value)
		AddHeader(header, "ce-"+k, string(value))
	}

	AddHeader(header, "Content-Type", event.DataContentType)
}

func AddHeader(header http.Header, key string, value string) {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)

	if value == "" {
		return
	}
	header.Add(key, value)
	Debug(3, "Setting header: %s: %s", key, value)
}
