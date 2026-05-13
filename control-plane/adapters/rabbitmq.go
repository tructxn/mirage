package adapters

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tructxn/mirage/control-plane/session"
)

type RabbitMQAdapter struct {
	url string
}

func NewRabbitMQAdapter(url string) *RabbitMQAdapter {
	return &RabbitMQAdapter{url: url}
}

func (a *RabbitMQAdapter) connect() (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.DialConfig(a.url, amqp.Config{Dial: amqp.DefaultDial(3 * time.Second)})
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, ch, nil
}

func (a *RabbitMQAdapter) PushRule(sessionID string, rule *session.Rule) error {
	exchange, ok := rule.Match["exchange"].(string)
	if !ok || exchange == "" {
		return fmt.Errorf("amqp rule missing match.exchange")
	}
	routingKey, _ := rule.Match["routing_key"].(string)
	body := fmt.Sprintf("%v", rule.Response)

	conn, ch, err := a.connect()
	if err != nil {
		return fmt.Errorf("rabbitmq connect: %w", err)
	}
	defer conn.Close()
	defer ch.Close()

	return ch.Publish(exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         []byte(body),
		DeliveryMode: amqp.Persistent,
		Headers: amqp.Table{
			"mirage-session": sessionID,
			"mirage-rule":    rule.ID,
		},
	})
}

func (a *RabbitMQAdapter) DeleteRule(sessionID string, rule session.Rule) error {
	return nil // AMQP messages are consumed — no delete concept
}

func (a *RabbitMQAdapter) Reset(sessionID string, rules []session.Rule) error {
	queues := map[string]bool{}
	for _, r := range rules {
		if q, ok := r.Match["queue"].(string); ok {
			queues[q] = true
		}
	}
	if len(queues) == 0 {
		return nil
	}
	conn, ch, err := a.connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	defer ch.Close()
	for q := range queues {
		_, _ = ch.QueuePurge(q, false)
	}
	return nil
}

func (a *RabbitMQAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	return nil, nil // Phase 4
}

func (a *RabbitMQAdapter) Healthy() bool {
	conn, err := amqp.DialConfig(a.url, amqp.Config{Dial: amqp.DefaultDial(2 * time.Second)})
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
