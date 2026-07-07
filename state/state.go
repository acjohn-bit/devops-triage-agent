package state

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    "log/slog"
    "os"
    "path/filepath"
    "sync"
    "time"

    _ "modernc.org/sqlite"
)

type State struct {
    TraceID      string   `json:"trace_id"`
    History      []string `json:"history"`
    LastTicketID string   `json:"last_ticket_id,omitempty"`
}

type Store struct {
    db *sql.DB
}

func OpenStateStore(path string) (*Store, error) {
    if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
        return nil, err
    }
    db, err := sql.Open("sqlite", path)
    if err != nil {
        return nil, err
    }
    if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS workflow_state (trace_id TEXT PRIMARY KEY, history TEXT, last_ticket_id TEXT)`); err != nil {
        db.Close()
        return nil, err
    }
    return &Store{db: db}, nil
}

func (s *Store) Close() error {
    if s == nil || s.db == nil {
        return nil
    }
    return s.db.Close()
}

func (s *Store) SaveState(st *State) error {
    if s == nil || s.db == nil || st == nil {
        return nil
    }
    historyJSON, err := json.Marshal(st.History)
    if err != nil {
        return err
    }
    _, err = s.db.Exec(`INSERT INTO workflow_state(trace_id, history, last_ticket_id) VALUES(?, ?, ?) ON CONFLICT(trace_id) DO UPDATE SET history=excluded.history, last_ticket_id=excluded.last_ticket_id`, st.TraceID, string(historyJSON), st.LastTicketID)
    return err
}

func (s *Store) LoadState() (*State, error) {
    if s == nil || s.db == nil {
        return NewState(), nil
    }
    var traceID, historyJSON, lastTicketID string
    err := s.db.QueryRow(`SELECT trace_id, history, last_ticket_id FROM workflow_state ORDER BY trace_id DESC LIMIT 1`).Scan(&traceID, &historyJSON, &lastTicketID)
    if err != nil {
        if err == sql.ErrNoRows {
            return NewState(), nil
        }
        return nil, err
    }
    var history []string
    if err := json.Unmarshal([]byte(historyJSON), &history); err != nil {
        return nil, err
    }
    return &State{TraceID: traceID, History: history, LastTicketID: lastTicketID}, nil
}

type AsyncStateSaver struct {
    store *Store
    queue chan *State
    wg    sync.WaitGroup
}

func NewAsyncStateSaver(store *Store) *AsyncStateSaver {
    if store == nil {
        return nil
    }
    s := &AsyncStateSaver{
        store: store,
        queue: make(chan *State, 4),
    }
    s.wg.Add(1)
    go s.run()
    return s
}

func (s *AsyncStateSaver) Save(st *State) {
    if s == nil || st == nil {
        return
    }
    copy := cloneState(st)
    select {
    case s.queue <- copy:
    default:
        select {
        case <-s.queue:
        default:
        }
        s.queue <- copy
    }
}

func (s *AsyncStateSaver) Close(ctx context.Context) error {
    if s == nil {
        return nil
    }
    close(s.queue)
    c := make(chan struct{})
    go func() {
        s.wg.Wait()
        close(c)
    }()
    select {
    case <-c:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}

func (s *AsyncStateSaver) run() {
    defer s.wg.Done()
    for st := range s.queue {
        if err := s.store.SaveState(st); err != nil {
            slog.Warn("workflow.state.persist_failed", "error", err)
        }
    }
}

func cloneState(st *State) *State {
    if st == nil {
        return nil
    }
    historyCopy := append([]string(nil), st.History...)
    return &State{
        TraceID:      st.TraceID,
        History:      historyCopy,
        LastTicketID: st.LastTicketID,
    }
}

func NewState() *State {
    return &State{TraceID: newTraceID(), History: []string{}}
}

func (s *State) AppendHistory(event string) {
    if s == nil || event == "" {
        return
    }
    s.History = append(s.History, event)
    if len(s.History) > 8 {
        s.History = compactHistory(s.History, 8)
    }
}

func compactHistory(history []string, limit int) []string {
    if len(history) <= limit {
        return append([]string(nil), history...)
    }
    older := len(history) - limit + 1
    compacted := make([]string, 0, limit)
    compacted = append(compacted, fmt.Sprintf("Earlier %d events omitted", older))
    compacted = append(compacted, history[len(history)-limit+1:]...)
    return compacted
}

// CompactHistory is an exported helper for tests and callers to compact an event slice.
func CompactHistory(history []string, limit int) []string {
    return compactHistory(history, limit)
}

func newTraceID() string {
    return fmt.Sprintf("trace-%d", time.Now().UnixNano())
}
