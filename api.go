package main

import (
	"encoding/json"
	"net/http"
)

type createResourceRequest struct {
	Kind string `json:"kind"` // "compute" | "network" | "storage"
	Spec string `json:"spec"` // free-form JSON, e.g. {"cpu":4,"memory_gb":16}
}

// RegisterAPIHandlers wires the client-facing control-plane API. Writes
// (create/terminate) are only accepted by the leader and only
// acknowledged after quorum commit; reads are served from local state for
// availability (may lag the leader by up to one heartbeat interval on a
// follower).
func RegisterAPIHandlers(mux *http.ServeMux, r *Raft, store *ResourceStore, rm *ResourceManager) {
	mux.HandleFunc("POST /resources", func(w http.ResponseWriter, req *http.Request) {
		var body createResourceRequest
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil || body.Kind == "" {
			http.Error(w, `bad request body, expected {"kind": "compute|network|storage", "spec": "..."}`, http.StatusBadRequest)
			return
		}
		res, err := rm.Create(body.Kind, body.Spec)
		writeResourceResult(w, res, err)
	})

	mux.HandleFunc("GET /resources", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(store.List())
	})

	mux.HandleFunc("GET /resources/{id}", func(w http.ResponseWriter, req *http.Request) {
		id := req.PathValue("id")
		res, ok := store.Get(id)
		if !ok {
			http.Error(w, "resource not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(res)
	})

	mux.HandleFunc("POST /resources/{id}/terminate", func(w http.ResponseWriter, req *http.Request) {
		id := req.PathValue("id")
		res, err := rm.Terminate(id)
		writeResourceResult(w, res, err)
	})

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(r.GetStatus())
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
}

func writeResourceResult(w http.ResponseWriter, res Resource, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		if nle, ok := err.(*NotLeaderError); ok {
			w.WriteHeader(http.StatusMisdirectedRequest) // 421: talk to the leader instead
			json.NewEncoder(w).Encode(map[string]string{
				"error": "not leader", "leader_id": nle.LeaderID,
			})
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(res)
}
