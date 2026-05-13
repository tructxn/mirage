package adapters

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/tructxn/mirage/control-plane/session"
)

type KafkaAdapter struct {
	brokers []string
}

func NewKafkaAdapter(broker string) *KafkaAdapter {
	return &KafkaAdapter{brokers: []string{broker}}
}

// PushRule produces a message to the configured topic so consumers receive it.
func (a *KafkaAdapter) PushRule(sessionID string, rule *session.Rule) error {
	topic, ok := rule.Match["topic"].(string)
	if !ok || topic == "" {
		return fmt.Errorf("kafka rule missing match.topic")
	}
	val := fmt.Sprintf("%v", rule.Response)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(a.brokers...),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	results := cl.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Value: []byte(val),
		Headers: []kgo.RecordHeader{
			{Key: "mirage-session", Value: []byte(sessionID)},
			{Key: "mirage-rule", Value: []byte(rule.ID)},
		},
	})
	return results.FirstErr()
}

func (a *KafkaAdapter) DeleteRule(sessionID string, rule session.Rule) error {
	// Kafka messages are immutable — no delete.
	return nil
}

func (a *KafkaAdapter) Reset(sessionID string, rules []session.Rule) error {
	topics := map[string]bool{}
	for _, r := range rules {
		if t, ok := r.Match["topic"].(string); ok && t != "" {
			topics[t] = true
		}
	}
	if len(topics) == 0 {
		return nil
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(a.brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	names := make([]string, 0, len(topics))
	for t := range topics {
		names = append(names, t)
	}
	_, _ = adm.DeleteTopics(ctx, names...)
	return nil
}

func (a *KafkaAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	return nil, nil // Phase 4
}

func (a *KafkaAdapter) Healthy() bool {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(a.brokers...),
		kgo.DialTimeout(2*time.Second),
	)
	if err != nil {
		return false
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return cl.Ping(ctx) == nil
}
