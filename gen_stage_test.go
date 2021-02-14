package ergo

import (
	"fmt"
	"testing"
	"time"

	"github.com/halturin/ergo/etf"
)

type GenStageProducerTest struct {
	GenStage
	sub GenStageSubscription
}

type GenStageConsumerTest struct {
	GenStage
	sub GenStageSubscription
}

// GenStage Producer
func (gs *GenStageProducerTest) InitStage(process *Process, args ...interface{}) (GenStageOptions, interface{}) {
	opts := GenStageOptions{}
	return opts, nil
}

func (gs *GenStageProducerTest) HandleDemand(subscription GenStageSubscription, demand uint, state interface{}) (error, []etf.Term, interface{}) {
	return nil, nil, state
}

func (gs *GenStageProducerTest) HandleEvents(subscription GenStageSubscription, events []etf.Term, state interface{}) (error, interface{}) {
	return nil, state
}

func (gs *GenStageProducerTest) HandleSubscribe(subscription GenStageSubscription, options GenStageSubscribeOptions, state interface{}) (error, interface{}) {
	fmt.Printf("got producer subs %#v \n", options)
	return nil, state
}

// GenStage Consumer
func (gs *GenStageConsumerTest) InitStage(process *Process, args ...interface{}) (GenStageOptions, interface{}) {
	opts := GenStageOptions{}
	return opts, nil
}

func (gs *GenStageConsumerTest) HandleDemand(subscription GenStageSubscription, demand uint, state interface{}) (error, []etf.Term, interface{}) {
	return nil, nil, state
}

func (gs *GenStageConsumerTest) HandleEvents(subscription GenStageSubscription, events []etf.Term, state interface{}) (error, interface{}) {
	return nil, state
}

func (gs *GenStageConsumerTest) HandleSubscribed(subscription GenStageSubscription, state interface{}) (error, bool, interface{}) {
	fmt.Printf("got consumer subs %#v \n", subscription)
	return nil, false, state
}

func TestGenStage(t *testing.T) {
	var sub GenStageSubscription

	fmt.Printf("\n=== Test GenStage\n")
	fmt.Printf("Starting node: nodeGenStage01@localhost...")

	node1 := CreateNode("nodeGenStage01@localhost", "cookies", NodeOptions{})

	if node1 == nil {
		t.Fatal("can't start node")
		return
	}

	producer := &GenStageProducerTest{}
	consumer := &GenStageConsumerTest{}
	producerProcess, err := node1.Spawn("stageProducer1", ProcessOptions{}, producer, nil)
	consumerProcess, err := node1.Spawn("stageConsumer1", ProcessOptions{}, consumer, nil)

	subOpts := GenStageSubscribeOptions{
		MinDemand: 10,
		MaxDemand: 20,
	}
	if sub, err = consumer.Subscribe(consumerProcess, "stageProducer1", subOpts); err != nil {
		t.Fatal(err)
	}

	producerProcess.Call(consumerProcess.Self(), "test")
	producerProcess.Cast(consumerProcess.Self(), "test")
	producerProcess.Send(consumerProcess.Self(), "test")

	consumer.sub = sub
	producer.sub = sub

	time.Sleep(1 * time.Second)
	fmt.Println("OKKK")

	node1.Stop()
}
