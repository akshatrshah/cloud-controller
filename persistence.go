package main

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"time"

	"github.com/lib/pq"
)

// PostgresStorage persists exactly what the Raft paper requires to survive
// a crash: currentTerm, votedFor, the log, and (as a practical addition
// beyond the strict paper spec) commitIndex, which lets a restarted node
// immediately re-apply its own previously-committed entries to the state
// machine instead of waiting to hear from a leader again.
//
// Each node writes to its own row set, keyed by node_id, so a single
// Postgres instance can safely back multiple Raft nodes in a demo/dev
// environment. In a production deployment you'd more likely give each
// node its own database (or at minimum its own schema) so no single
// Postgres instance is itself a cluster-wide single point of failure --
// see the README's "Known limitations" section for that tradeoff spelled
// out explicitly.
type PostgresStorage struct {
	db     *sql.DB
	nodeID string
}

func NewPostgresStorage(dsn, nodeID string) (*PostgresStorage, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening postgres connection: %w", err)
	}
	// Raft writes are serialized per node (they happen under the Raft
	// mutex), so a handful of connections is plenty. Capping the pool
	// keeps 5 nodes well under Postgres's default max_connections=100.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	db.SetConnMaxIdleTime(5 * time.Minute)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}

	ps := &PostgresStorage{db: db, nodeID: nodeID}
	if err := ps.migrate(); err != nil {
		return nil, fmt.Errorf("running migrations: %w", err)
	}
	return ps, nil
}

// migrationLockID is an arbitrary constant used with pg_advisory_lock to
// serialize schema creation. Without this, multiple nodes starting up
// simultaneously and all running `CREATE TABLE IF NOT EXISTS` at once can
// race on Postgres's own system catalog (a well-known Postgres gotcha,
// not specific to this schema) and fail with a spurious duplicate-key
// error on pg_type. Discovered by actually starting a 5-node cluster
// concurrently against a shared Postgres instance during testing.
const migrationLockID = 8891234

func (ps *PostgresStorage) migrate() error {
	if _, err := ps.db.Exec(`SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("acquiring migration lock: %w", err)
	}
	defer ps.db.Exec(`SELECT pg_advisory_unlock($1)`, migrationLockID)

	_, err := ps.db.Exec(`
		CREATE TABLE IF NOT EXISTS raft_meta (
			node_id      TEXT PRIMARY KEY,
			current_term INTEGER NOT NULL DEFAULT 0,
			voted_for    TEXT NOT NULL DEFAULT '',
			commit_index INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS raft_log (
			node_id      TEXT NOT NULL,
			log_index    INTEGER NOT NULL,
			term         INTEGER NOT NULL,
			command_json TEXT NOT NULL,
			PRIMARY KEY (node_id, log_index)
		);
	`)
	return err
}

func (ps *PostgresStorage) SaveMeta(term int, votedFor string, commitIndex int) error {
	_, err := ps.db.Exec(`
		INSERT INTO raft_meta (node_id, current_term, voted_for, commit_index)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (node_id) DO UPDATE
		SET current_term = EXCLUDED.current_term,
		    voted_for    = EXCLUDED.voted_for,
		    commit_index = EXCLUDED.commit_index
	`, ps.nodeID, term, votedFor, commitIndex)
	return err
}

func (ps *PostgresStorage) LoadMeta() (term int, votedFor string, commitIndex int, found bool, err error) {
	row := ps.db.QueryRow(`
		SELECT current_term, voted_for, commit_index FROM raft_meta WHERE node_id = $1
	`, ps.nodeID)
	err = row.Scan(&term, &votedFor, &commitIndex)
	if err == sql.ErrNoRows {
		return 0, "", 0, false, nil
	}
	if err != nil {
		return 0, "", 0, false, err
	}
	return term, votedFor, commitIndex, true, nil
}

// AppendLog writes a batch of entries with ONE multi-row INSERT (one
// network round trip, one fsync on commit) instead of one statement per
// entry. With group commit on the leader and batched AppendEntries on
// followers, a single call commonly carries dozens to hundreds of entries.
func (ps *PostgresStorage) AppendLog(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	indexes := make([]int64, len(entries))
	terms := make([]int64, len(entries))
	cmds := make([]string, len(entries))
	for i, e := range entries {
		cmdJSON, err := json.Marshal(e.Command)
		if err != nil {
			return err
		}
		indexes[i] = int64(e.Index)
		terms[i] = int64(e.Term)
		cmds[i] = string(cmdJSON)
	}
	_, err := ps.db.Exec(`
		INSERT INTO raft_log (node_id, log_index, term, command_json)
		SELECT $1, idx, term, cmd
		FROM unnest($2::int[], $3::int[], $4::text[]) AS t(idx, term, cmd)
		ON CONFLICT (node_id, log_index) DO UPDATE
		SET term = EXCLUDED.term, command_json = EXCLUDED.command_json
	`, ps.nodeID, pq.Array(indexes), pq.Array(terms), pq.Array(cmds))
	return err
}

func (ps *PostgresStorage) TruncateLogFrom(index int) error {
	_, err := ps.db.Exec(`DELETE FROM raft_log WHERE node_id = $1 AND log_index >= $2`, ps.nodeID, index)
	return err
}

func (ps *PostgresStorage) LoadLog() ([]LogEntry, error) {
	rows, err := ps.db.Query(`
		SELECT log_index, term, command_json FROM raft_log
		WHERE node_id = $1 ORDER BY log_index ASC
	`, ps.nodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Index 0 is always the in-memory sentinel entry -- never persisted,
	// since it represents "no entries yet" rather than a real command.
	log := []LogEntry{{Term: 0, Index: 0}}
	for rows.Next() {
		var idx, term int
		var cmdJSON string
		if err := rows.Scan(&idx, &term, &cmdJSON); err != nil {
			return nil, err
		}
		var cmd Command
		if err := json.Unmarshal([]byte(cmdJSON), &cmd); err != nil {
			return nil, err
		}
		log = append(log, LogEntry{Term: term, Index: idx, Command: cmd})
	}
	return log, rows.Err()
}

func (ps *PostgresStorage) Close() error {
	return ps.db.Close()
}
