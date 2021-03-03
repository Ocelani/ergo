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
func (gs *GenStageProducerTest) HandleDemand(subscription GenStageSubscription, count uint, state interface{}) (error, etf.List, interface{}) {
	Events := etf.List{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
	}
	return nil, Events, state
}

func (gs *GenStageProducerTest) HandleSubscribe(subscription GenStageSubscription, options GenStageSubscribeOptions, state interface{}) (error, interface{}) {
	fmt.Printf("got producer subs %#v \n", options)
	return nil, state
}

// a simple GenStage Consumer
func (gs *GenStageConsumerTest) HandleEvents(subscription GenStageSubscription, events etf.List, state interface{}) (error, interface{}) {
	return nil, state
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
	node1.Spawn("stageProducer", ProcessOptions{}, producer, nil)
	consumer1Process, _ := node1.Spawn("stageConsumer1", ProcessOptions{}, consumer, nil)
	consumer2Process, _ := node1.Spawn("stageConsumer2", ProcessOptions{}, consumer, nil)
	consumer3Process, _ := node1.Spawn("stageConsumer3", ProcessOptions{}, consumer, nil)

	subOpts := GenStageSubscribeOptions{
		MinDemand: 2,
		MaxDemand: 20,
	}
	consumer.Subscribe(consumer1Process, "stageProducer", subOpts)
	consumer.Subscribe(consumer2Process, "stageProducer", subOpts)
	consumer.Subscribe(consumer3Process, "stageProducer", subOpts)
	consumer.Subscribe(consumer3Process, "stageProducer", subOpts)

	time.Sleep(1 * time.Second)
	fmt.Println("OKKK")

	node1.Stop()
}
