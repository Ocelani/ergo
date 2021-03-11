package ergo

import (
	"fmt"
	"testing"
	"time"

	"github.com/halturin/ergo/etf"
)

type GenStageProducerTest struct {
	GenStage
}

type GenStageConsumerTest struct {
	GenStage
}

// a simple GenStage Producer
func (gs *GenStageProducerTest) HandleDemand(subscription GenStageSubscription, count uint, state interface{}) (error, etf.List) {
	fmt.Printf("producer got demand %d from sub %v\n", count, subscription)
	return nil, nil
}

func (gs *GenStageProducerTest) HandleSubscribe(subscription GenStageSubscription, options GenStageSubscribeOptions, state interface{}) error {
	fmt.Printf("producer got sub %v with opt %v\n", subscription, options)
	return nil
}

// a simple GenStage Consumer
func (gs *GenStageConsumerTest) HandleEvents(subscription GenStageSubscription, events etf.List, state interface{}) error {
	fmt.Println("consumer got events", events)
	return nil
}

func TestGenStageSimple(t *testing.T) {

	fmt.Printf("\n=== Test GenStageSimple\n")
	fmt.Printf("Starting node: nodeGenStageSimple01@localhost...")

	node1 := CreateNode("nodeGenStageSimple01@localhost", "cookies", NodeOptions{})

	if node1 == nil {
		t.Fatal("can't start node")
		return
	}

	producer := &GenStageProducerTest{}
	consumer := &GenStageConsumerTest{}
	producerProcess, _ := node1.Spawn("stageProducer", ProcessOptions{}, producer, nil)
	consumer1Process, _ := node1.Spawn("stageConsumer1", ProcessOptions{}, consumer, nil)
	consumer2Process, _ := node1.Spawn("stageConsumer2", ProcessOptions{}, consumer, nil)
	consumer3Process, _ := node1.Spawn("stageConsumer3", ProcessOptions{}, consumer, nil)

	subOpts := GenStageSubscribeOptions{
		MinDemand:    4,
		MaxDemand:    5,
		ManualDemand: true,
	}
	consumer.Subscribe(consumer1Process, "stageProducer", subOpts)
	sub := consumer.Subscribe(consumer2Process, "stageProducer", subOpts)
	consumer.Subscribe(consumer3Process, "stageProducer", subOpts)
	sub1 := consumer.Subscribe(consumer3Process, "stageProducer", subOpts)

	time.Sleep(1 * time.Second)
	consumer.Ask(consumer2Process, sub, 1)

	Events := etf.List{
		1, 2, 3,
	}
	producer.SendEvents(producerProcess, Events)
	time.Sleep(1 * time.Second)

	Events = etf.List{
		"k", "m", "c", "d", "o", "f", "v", "a",
	}
	producer.SendEvents(producerProcess, Events)
	time.Sleep(1 * time.Second)
	consumer.Ask(consumer3Process, sub1, 1)
	time.Sleep(1 * time.Second)
	node1.Stop()
}
