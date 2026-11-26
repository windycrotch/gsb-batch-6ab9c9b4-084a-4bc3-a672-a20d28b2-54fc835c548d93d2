package sfu

import (
	"io"
	"net/http"

	"google.golang.org/protobuf/proto"
)

const protobufContentType = "application/x-protobuf"

// HandleServers implements the /servers protobuf API.
// GET returns the current set of relay servers, POST replaces the intended set.
func (s *Sfu) HandleServers(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Content-Type") != protobufContentType {
		return false
	}

	switch r.Method {
	case http.MethodGet:
		s.writeProto(w, s.GetCurrentServers())
		return true
	case http.MethodPost:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read payload", http.StatusInternalServerError)
			return true
		}

		var update CurrentServers
		if err := proto.Unmarshal(body, &update); err != nil {
			http.Error(w, "Failed to deserialize payload", http.StatusInternalServerError)
			return true
		}

		s.writeProto(w, s.SetCurrentServers(&update))
		return true
	}

	return false
}

// HandleStatus implements the /status protobuf API.
func (s *Sfu) HandleStatus(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Content-Type") != protobufContentType {
		return false
	}

	s.writeProto(w, s.GetStatus())
	return true
}

func (s *Sfu) writeProto(w http.ResponseWriter, message proto.Message) {
	data, err := proto.Marshal(message)
	if err != nil {
		http.Error(w, "Failed to serialize Protobuf", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", protobufContentType)
	w.WriteHeader(http.StatusOK)

	w.Write(data)
}
