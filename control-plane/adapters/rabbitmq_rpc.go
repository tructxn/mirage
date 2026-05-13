package adapters

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tructxn/mirage/control-plane/session"
)

// RabbitMQRPCAdapter mocks the server side of RabbitMQ RPC.
// It consumes from configured queues and replies to replyTo with the configured response.
type RabbitMQRPCAdapter struct {
	url      string
	mu       sync.Mutex
	sessions map[string]*rpcSession
}

type rpcSession struct {
	rules     []session.Rule
	consumers map[string]context.CancelFunc // queue → goroutine cancel func
	traffic   []session.TrafficRecord
}

func NewRabbitMQRPCAdapter(url string) *RabbitMQRPCAdapter {
	return &RabbitMQRPCAdapter{
		url:      url,
		sessions: make(map[string]*rpcSession),
	}
}

func (a *RabbitMQRPCAdapter) getOrCreateSession(sessionID string) *rpcSession {
	sess, ok := a.sessions[sessionID]
	if !ok {
		sess = &rpcSession{consumers: make(map[string]context.CancelFunc)}
		a.sessions[sessionID] = sess
	}
	return sess
}

// PushRule registers the rule and starts a consumer goroutine on match.queue
// if one is not already running for this session+queue.
func (a *RabbitMQRPCAdapter) PushRule(sessionID string, rule *session.Rule) error {
	queue, ok := rule.Match["queue"].(string)
	if !ok || queue == "" {
		return fmt.Errorf("amqp-rpc rule missing match.queue")
	}

	a.mu.Lock()
	sess := a.getOrCreateSession(sessionID)
	sess.rules = append(sess.rules, *rule)
	_, alreadyConsuming := sess.consumers[queue]
	var ctx context.Context
	var cancel context.CancelFunc
	if !alreadyConsuming {
		ctx, cancel = context.WithCancel(context.Background())
		sess.consumers[queue] = cancel
	}
	a.mu.Unlock()

	if !alreadyConsuming {
		go a.runConsumer(ctx, sessionID, queue)
	}
	return nil
}

func (a *RabbitMQRPCAdapter) DeleteRule(sessionID string, rule session.Rule) error {
	queue, _ := rule.Match["queue"].(string)

	a.mu.Lock()
	defer a.mu.Unlock()
	sess, ok := a.sessions[sessionID]
	if !ok {
		return nil
	}
	var filtered []session.Rule
	for _, r := range sess.rules {
		if r.ID != rule.ID {
			filtered = append(filtered, r)
		}
	}
	sess.rules = filtered

	if queue != "" {
		hasMore := false
		for _, r := range sess.rules {
			if q, _ := r.Match["queue"].(string); q == queue {
				hasMore = true
				break
			}
		}
		if !hasMore {
			if cancel, ok := sess.consumers[queue]; ok {
				cancel()
				delete(sess.consumers, queue)
			}
		}
	}
	return nil
}

func (a *RabbitMQRPCAdapter) Reset(sessionID string, rules []session.Rule) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	sess, ok := a.sessions[sessionID]
	if !ok {
		return nil
	}
	for _, cancel := range sess.consumers {
		cancel()
	}
	delete(a.sessions, sessionID)
	return nil
}

func (a *RabbitMQRPCAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	sess, ok := a.sessions[sessionID]
	if !ok {
		return nil, nil
	}
	out := make([]session.TrafficRecord, len(sess.traffic))
	copy(out, sess.traffic)
	return out, nil
}

func (a *RabbitMQRPCAdapter) Healthy() bool {
	conn, err := amqp.DialConfig(a.url, amqp.Config{Dial: amqp.DefaultDial(2 * time.Second)})
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// matchRule evaluates rules against an incoming delivery.
// rules must already be sorted by priority descending by the caller.
// Returns the first matching rule, or zero value + false if none match.
func matchRule(rules []session.Rule, d amqp.Delivery) (session.Rule, bool) {
	for _, rule := range rules {
		if ruleMatches(rule, d) {
			return rule, true
		}
	}
	return session.Rule{}, false
}

// ruleMatches returns true if all non-queue match fields satisfy the delivery.
func ruleMatches(rule session.Rule, d amqp.Delivery) bool {
	for k, v := range rule.Match {
		if k == "queue" {
			continue
		}
		expected, _ := v.(string)
		var actual string
		switch {
		case k == "properties.type":
			actual = d.Type
		case k == "properties.content_type":
			actual = d.ContentType
		case strings.HasPrefix(k, "headers."):
			headerKey := strings.TrimPrefix(k, "headers.")
			if hv, ok := d.Headers[headerKey]; ok {
				actual = fmt.Sprintf("%v", hv)
			}
		default:
			continue // unknown field — ignore
		}
		if actual != expected {
			return false
		}
	}
	return true
}

// nearMissField returns the first match field (by map iteration) that fails for the delivery.
func nearMissField(rule session.Rule, d amqp.Delivery) string {
	for k, v := range rule.Match {
		if k == "queue" {
			continue
		}
		expected, _ := v.(string)
		var actual string
		switch {
		case k == "properties.type":
			actual = d.Type
		case k == "properties.content_type":
			actual = d.ContentType
		case strings.HasPrefix(k, "headers."):
			headerKey := strings.TrimPrefix(k, "headers.")
			if hv, ok := d.Headers[headerKey]; ok {
				actual = fmt.Sprintf("%v", hv)
			}
		}
		if actual != expected {
			return k
		}
	}
	return "unknown"
}

func (a *RabbitMQRPCAdapter) runConsumer(ctx context.Context, sessionID, queue string) {
	defer func() {
		a.mu.Lock()
		if sess, ok := a.sessions[sessionID]; ok {
			if cancel, exists := sess.consumers[queue]; exists {
				cancel() // no-op if already cancelled
				delete(sess.consumers, queue)
			}
		}
		a.mu.Unlock()
	}()

	conn, err := amqp.DialConfig(a.url, amqp.Config{Dial: amqp.DefaultDial(5 * time.Second)})
	if err != nil {
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return
	}
	defer ch.Close()

	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			a.handleDelivery(ch, sessionID, queue, d)
		}
	}
}

func (a *RabbitMQRPCAdapter) handleDelivery(ch *amqp.Channel, sessionID, queue string, d amqp.Delivery) {
	start := time.Now()

	a.mu.Lock()
	var queueRules []session.Rule
	if sess, ok := a.sessions[sessionID]; ok {
		for _, r := range sess.rules {
			if q, _ := r.Match["queue"].(string); q == queue {
				queueRules = append(queueRules, r)
			}
		}
	}
	a.mu.Unlock()

	reqMap := map[string]any{
		"queue":          queue,
		"correlation_id": d.CorrelationId,
		"reply_to":       d.ReplyTo,
	}
	if d.Type != "" {
		reqMap["properties.type"] = d.Type
	}
	if d.ContentType != "" {
		reqMap["properties.content_type"] = d.ContentType
	}
	if len(d.Headers) > 0 {
		headers := map[string]string{}
		for k, v := range d.Headers {
			headers[k] = fmt.Sprintf("%v", v)
		}
		reqMap["headers"] = headers
	}

	rec := session.TrafficRecord{
		Timestamp: time.Now().UTC(),
		Protocol:  "amqp-rpc",
		Request:   reqMap,
	}

	// Sort by priority descending so both matchRule and near-miss use the same order.
	sortedRules := make([]session.Rule, len(queueRules))
	copy(sortedRules, queueRules)
	for i := 1; i < len(sortedRules); i++ {
		for j := i; j > 0 && sortedRules[j].Priority > sortedRules[j-1].Priority; j-- {
			sortedRules[j], sortedRules[j-1] = sortedRules[j-1], sortedRules[j]
		}
	}

	matched, found := matchRule(sortedRules, d)
	if found {
		body := fmt.Sprintf("%v", matched.Response)
		if d.ReplyTo != "" {
			_ = ch.Publish("", d.ReplyTo, false, false, amqp.Publishing{
				ContentType:   "application/json",
				CorrelationId: d.CorrelationId,
				Body:          []byte(body),
			})
		}
		id := matched.ID
		rec.MatchedRuleID = &id
		rec.Response = body
	} else if len(sortedRules) > 0 {
		closest := sortedRules[0]
		rec.NearMiss = &session.NearMiss{
			RuleID:      closest.ID,
			FailedField: nearMissField(closest, d),
		}
	}

	rec.DurationMS = time.Since(start).Milliseconds()
	_ = d.Ack(false)

	a.mu.Lock()
	if sess, ok := a.sessions[sessionID]; ok {
		sess.traffic = append(sess.traffic, rec)
	}
	a.mu.Unlock()
}
