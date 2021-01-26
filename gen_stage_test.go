package ergo

import (
	"fmt"
	"testing"

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
	opts := GenStageOptions{
		stageType: GenStageTypeProducer,
		demand:    GenStageDemandModeForward,
	}
	return opts, nil
}

func (gs *GenStageProducerTest) HandleCancel(cancelReason etf.Term, from etf.Term, state interface{}) (string, etf.Term, interface{}) {
	return "noreply", "subscription", state
}

func (gs *GenStageProducerTest) HandleDemand(demand uint64, state interface{}) (string, etf.Term, interface{}) {
	return "noreply", 1, state
}

func (gs *GenStageProducerTest) HandleEvents(events interface{}, from etf.Term, state interface{}) (string, interface{}) {
	return "asdf", state
}

func (gs *GenStageProducerTest) HandleSubscribe(stageType GenStageType, options etf.List, state interface{}) (GenStageSubscriptionMode, interface{}) {
	fmt.Println("got subs")
	return GenStageSubscriptionModeAuto, state
}

// GenStage Consumer
func (gs *GenStageConsumerTest) InitStage(process *Process, args ...interface{}) (GenStageOptions, interface{}) {
	opts := GenStageOptions{
		stageType: GenStageTypeConsumer,
	}
	return opts, nil
}

func (gs *GenStageConsumerTest) HandleCancel(cancelReason etf.Term, from etf.Term, state interface{}) (string, etf.Term, interface{}) {
	return "noreply", "subscription", state
}

func (gs *GenStageConsumerTest) HandleDemand(demand uint64, state interface{}) (string, etf.Term, interface{}) {
	return "noreply", 1, state
}

func (gs *GenStageConsumerTest) HandleEvents(events interface{}, from etf.Term, state interface{}) (string, interface{}) {
	return "asdf", state
}

func (gs *GenStageConsumerTest) HandleSubscribe(stageType GenStageType, options etf.List, state interface{}) (GenStageSubscriptionMode, interface{}) {
	return GenStageSubscriptionModeAuto, state
}

func TestGenStage(t *testing.T) {
	var err error
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
	_, err = node1.Spawn("stageProducer1", ProcessOptions{}, producer, nil)
	consumerProcess, err := node1.Spawn("stageConsumer1", ProcessOptions{}, consumer, nil)

	subOpts := GenStageSubscriptionOptions{
		Mode: GenStageSubscriptionModeAuto,
	}
	if sub, err = consumer.Subscribe(consumerProcess, "stageProducer1", subOpts); err != nil {
		t.Fatal(err)
	}

	consumer.sub = sub
	producer.sub = sub

	fmt.Println("OK")

	node1.Stop()
}
