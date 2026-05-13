package session

import (
	"sync"

	"github.com/google/uuid"
)

type Store struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func NewStore() *Store {
	return &Store{sessions: make(map[string]*Session)}
}

func (s *Store) Create() *Session {
	sess := &Session{ID: uuid.NewString()}
	s.mu.Lock()
	s.sessions[sess.ID] = sess
	s.mu.Unlock()
	return sess
}

func (s *Store) Get(id string) *Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions[id]
}

func (s *Store) Delete(id string) {
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
}

func (s *Store) AddRule(sessionID string, rule Rule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.sessions[sessionID]; ok {
		sess.Rules = append(sess.Rules, rule)
	}
}

func (s *Store) DeleteRule(sessionID, ruleID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return
	}
	filtered := sess.Rules[:0]
	for _, r := range sess.Rules {
		if r.ID != ruleID {
			filtered = append(filtered, r)
		}
	}
	sess.Rules = filtered
}

func (s *Store) Rules(sessionID string) []Rule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return nil
	}
	out := make([]Rule, len(sess.Rules))
	copy(out, sess.Rules)
	return out
}

func (s *Store) RuleByBackendID(sessionID, backendID string) (Rule, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return Rule{}, false
	}
	for i := range sess.Rules {
		if sess.Rules[i].BackendID == backendID {
			r := sess.Rules[i] // copy
			return r, true
		}
	}
	return Rule{}, false
}
