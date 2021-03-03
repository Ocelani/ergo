package ergo

import (
	"fmt"
	"github.com/halturin/ergo/etf"
)

// GenStageDispatcherBehaviour defined interface for the dispatcher
// implementation.
type GenStageDispatcherBehaviour interface {
	// InitStageDispatcher(opts)
	Init(opts GenStageOptions) interface{}

	// Ask called every time a consumer sends demand
	Ask(subscription GenStageSubscription, count uint, state interface{}) interface{}

	// Cancel called every time a subscription is cancelled or the consumer goes down.
	Cancel(subscription GenStageSubscription, state interface{}) interface{}

	// Dispatch called every time a producer wants to dispatch an event.
	Dispatch(events etf.List, state interface{}) ([]GenStageDispatchItem, interface{})

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

type GenStageDispatchItem struct {
	subscription GenStageSubscription
	events       etf.List
}

// Dispatcher Demand implementation

type demand struct {
	subscription GenStageSubscription
	minDemand    uint
	maxDemand    uint
	n            uint
}

type demandState struct {
	demands map[etf.Pid]*demand
	order   []etf.Pid
	i       int
	// buffer of events
	events         chan etf.Term
	bufferSize     uint
	bufferKeepLast bool
}

func (dd *dispatcherDemand) Init(opts GenStageOptions) interface{} {
	state := &demandState{
		demands:        make(map[etf.Pid]*demand),
		i:              0,
		events:         make(chan etf.Term, opts.bufferSize),
		bufferSize:     opts.bufferSize,
		bufferKeepLast: opts.bufferKeepLast,
	}
	return state
}

func (dd *dispatcherDemand) Ask(subscription GenStageSubscription, count uint, state interface{}) interface{} {
	finalState := state.(*demandState)
	demand, ok := finalState.demands[subscription.Pid]
	if !ok {
		return state
	}
	demand.n += count

	return finalState
}

func (dd *dispatcherDemand) Cancel(subscription GenStageSubscription, state interface{}) interface{} {
	finalState := state.(*demandState)
	delete(finalState.demands, subscription.Pid)
	for i := range finalState.order {
		if finalState.order[i] != subscription.Pid {
			continue
		}
		finalState.order[i] = finalState.order[0]
		finalState.order = finalState.order[1:]
		break
	}
	return finalState
}

func (dd *dispatcherDemand) Dispatch(events etf.List, state interface{}) ([]GenStageDispatchItem, interface{}) {
	fmt.Println("DISPATCHING", events)
	// ignore empty event list
	if len(events) == 0 {
		return nil, state
	}

	finalState := state.(*demandState)
	// put events into the buffer before we start dispatching
	for e := range events {
		select {
		case finalState.events <- events[e]:
			fmt.Println("DISPATCHING put into channel", events[e], len(finalState.events))
			continue
		default:
			fmt.Println("DISPATCHING buffer is full", events[e], len(finalState.events))
			// buffer is full
			if finalState.bufferKeepLast {
				<-finalState.events
				finalState.events <- events[e]
				continue
			}
		}
		// seems we dont have enough space to keep these events.
		break
	}

	fmt.Println("DISPATCHING ORDER", finalState.order)
	// check out whether we have subscribers
	if len(finalState.order) == 0 {
		return nil, finalState
	}

	dispatchItems := []GenStageDispatchItem{}
	for range finalState.order {
		if len(finalState.events) == 0 {
			// have nothing to dispatch
			break
		}
		if finalState.i > len(finalState.order)-1 {
			finalState.i = 0
		}

		pid := finalState.order[finalState.i]
		demand := finalState.demands[pid]
		finalState.i++

		if demand.n < demand.minDemand || len(finalState.events) < int(demand.minDemand) {
			continue
		}

		item := makeDispatchItem(finalState.events, demand)
		dispatchItems = append(dispatchItems, item)
	}

	fmt.Println("DISPATCHING ITEMS", dispatchItems)
	return dispatchItems, finalState
}

func makeDispatchItem(events chan etf.Term, d *demand) GenStageDispatchItem {
	item := GenStageDispatchItem{
		subscription: d.subscription,
	}

	i := uint(0)
	for {
		if d.n == 0 || i == d.maxDemand {
			// this subscription dont have demand anymore
			// or limit has reached
			break
		}
		select {
		case e := <-events:
			item.events = append(item.events, e)
			d.n--
			i++
			continue
		default:
			// we dont have events in the buffer
		}

		break
	}

	return item
}

func (dd *dispatcherDemand) Subscribe(subscription GenStageSubscription, opts GenStageSubscribeOptions, state interface{}) interface{} {
	finalState := state.(*demandState)
	newDemand := &demand{
		subscription: subscription,
		minDemand:    opts.MinDemand,
		maxDemand:    opts.MaxDemand,
	}
	finalState.demands[subscription.Pid] = newDemand
	finalState.order = append(finalState.order, subscription.Pid)
	return finalState
}

// Dispatcher Broadcast implementation

func (db *dispatcherBroadcast) Init(opts GenStageOptions) interface{} {
	return nil
}

func (db *dispatcherBroadcast) Ask(subscription GenStageSubscription, count uint, state interface{}) interface{} {
	return state
}

func (db *dispatcherBroadcast) Cancel(subscription GenStageSubscription, state interface{}) interface{} {
	return state
}

func (db *dispatcherBroadcast) Dispatch(events etf.List, state interface{}) ([]GenStageDispatchItem, interface{}) {
	return nil, state
}

func (db *dispatcherBroadcast) Subscribe(subscription GenStageSubscription, opts GenStageSubscribeOptions, state interface{}) interface{} {
	return state
}

// Dispatcher Partition implementation

func (dp *dispatcherPartition) Init(opts GenStageOptions) interface{} {
	return nil
}

func (dp *dispatcherPartition) Ask(subscription GenStageSubscription, count uint, state interface{}) interface{} {
	return state
}

func (dp *dispatcherPartition) Cancel(subscription GenStageSubscription, state interface{}) interface{} {
	return state
}

func (dp *dispatcherPartition) Dispatch(events etf.List, state interface{}) ([]GenStageDispatchItem, interface{}) {
	return nil, state
}

func (dp *dispatcherPartition) Subscribe(subscription GenStageSubscription, opts GenStageSubscribeOptions, state interface{}) interface{} {
	return state
}
