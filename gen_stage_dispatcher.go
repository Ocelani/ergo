package ergo

import (
	"github.com/halturin/ergo/etf"
	//"fmt"
)

// GenStageDispatcherBehaviour defined interface for the dispatcher
// implementation.
type GenStageDispatcherBehaviour interface {
	// InitStageDispatcher(opts)
	Init(opts GenStageOptions) interface{}

	// Ask called every time a consumer sends demand
	Ask(subscription GenStageSubscription, demand uint, state interface{}) interface{}

	// Cancel called every time a subscription is cancelled or the consumer goes down.
	Cancel(subscription GenStageSubscription, state interface{}) interface{}

	// Dispatch called every time a producer wants to dispatch an event.
	Dispatch(events etf.List, state interface{}) interface{}

	// Subscribe called every time the producer gets a new subscriber
	Subscribe(subscription GenStageSubscription, opts GenStageSubscribeOptions, state interface{}) interface{}
}

type GenStageDispatcher int
type dispatcherDemand struct{}
type dispatcherBroadcast struct{}
type dispatcherPartition struct{}

const (
	GenStageDispatcherDemand    GenStageDispatcher = 0
	GenStageDispatcherBroadcast GenStageDispatcher = 1
	GenStageDispatcherPartition GenStageDispatcher = 2
)

// CreateGenStageDispatcher creates a new dispatcher with a given type.
// There are 3 type of dispatchers we have implemented
//		GenStageDispatcherDemand
//			A dispatcher that sends batches to the highest demand.
//			This is the default dispatcher used by GenStage. In
//			order to avoid greedy consumers, it is recommended
//			that all consumers have exactly the same maximum demand.
//		GenStageDispatcherBroadcast
//			A dispatcher that accumulates demand from all consumers
//			before broadcasting events to all of them.
//			This dispatcher guarantees that events are dispatched to
//			all consumers without exceeding the demand of any given consumer.
//			The demand is only sent upstream once all consumers ask for data.
//		GenStageDispatcherPartition
//			A dispatcher that sends events according to partitions.
//			Keep in mind that, if partitions are not evenly distributed,
//			a backed-up partition will slow all other ones
//
//		To create a custom dispatcher you should implement GenStageDispatcherBehaviour interface
func CreateGenStageDispatcher(dispatcher GenStageDispatcher) GenStageDispatcherBehaviour {
	switch dispatcher {
	case GenStageDispatcherDemand:
		return &dispatcherDemand{}
	case GenStageDispatcherBroadcast:
		return &dispatcherBroadcast{}
	case GenStageDispatcherPartition:
		return &dispatcherPartition{}
	}

	return nil
}

// Dispatcher Demand implementation

func (dd *dispatcherDemand) Init(opts GenStageOptions) interface{} {
	return nil
}

func (dd *dispatcherDemand) Ask(subscription GenStageSubscription, demand uint, state interface{}) interface{} {
	return state
}

func (dd *dispatcherDemand) Cancel(subscription GenStageSubscription, state interface{}) interface{} {
	return state
}

func (dd *dispatcherDemand) Dispatch(events etf.List, state interface{}) interface{} {
	return state
}

func (dd *dispatcherDemand) Subscribe(subscription GenStageSubscription, opts GenStageSubscribeOptions, state interface{}) interface{} {
	return state
}

// Dispatcher Broadcast implementation

func (db *dispatcherBroadcast) Init(opts GenStageOptions) interface{} {
	return nil
}

func (db *dispatcherBroadcast) Ask(subscription GenStageSubscription, demand uint, state interface{}) interface{} {
	return state
}

func (db *dispatcherBroadcast) Cancel(subscription GenStageSubscription, state interface{}) interface{} {
	return state
}

func (db *dispatcherBroadcast) Dispatch(events etf.List, state interface{}) interface{} {
	return state
}

func (db *dispatcherBroadcast) Subscribe(subscription GenStageSubscription, opts GenStageSubscribeOptions, state interface{}) interface{} {
	return state
}

// Dispatcher Partition implementation

func (dp *dispatcherPartition) Init(opts GenStageOptions) interface{} {
	return nil
}

func (dp *dispatcherPartition) Ask(subscription GenStageSubscription, demand uint, state interface{}) interface{} {
	return state
}

func (dp *dispatcherPartition) Cancel(subscription GenStageSubscription, state interface{}) interface{} {
	return state
}

func (dp *dispatcherPartition) Dispatch(events etf.List, state interface{}) interface{} {
	return state
}

func (dp *dispatcherPartition) Subscribe(subscription GenStageSubscription, opts GenStageSubscribeOptions, state interface{}) interface{} {
	return state
}
