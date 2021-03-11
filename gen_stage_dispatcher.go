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
	Ask(subscription GenStageSubscription, count uint, state interface{})

	// Cancel called every time a subscription is cancelled or the consumer goes down.
	Cancel(subscription GenStageSubscription, state interface{})

	// Dispatch called every time a producer wants to dispatch an event.
	Dispatch(events etf.List, state interface{}) []GenStageDispatchItem

	// Subscribe called every time the producer gets a new subscriber
	Subscribe(subscription GenStageSubscription, opts GenStageSubscribeOptions, state interface{})
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

func (dd *dispatcherDemand) Ask(subscription GenStageSubscription, count uint, state interface{}) {
	st := state.(*demandState)
	fmt.Println("DISPATCHING got ASK", count)
	demand, ok := st.demands[subscription.Pid]
	if !ok {
		return
	}
	demand.n += count
	return
}

func (dd *dispatcherDemand) Cancel(subscription GenStageSubscription, state interface{}) {
	st := state.(*demandState)
	delete(st.demands, subscription.Pid)
	for i := range st.order {
		if st.order[i] != subscription.Pid {
			continue
		}
		st.order[i] = st.order[0]
		st.order = st.order[1:]
		break
	}
	return
}

func (dd *dispatcherDemand) Dispatch(events etf.List, state interface{}) []GenStageDispatchItem {
	st := state.(*demandState)
	// put events into the buffer before we start dispatching
	for e := range events {
		select {
		case st.events <- events[e]:
			continue
		default:
			// buffer is full
			if st.bufferKeepLast {
				<-st.events
				st.events <- events[e]
				continue
			}
		}
		// seems we dont have enough space to keep these events.
		break
	}

	// check out whether we have subscribers
	if len(st.order) == 0 {
		return nil
	}

	dispatchItems := []GenStageDispatchItem{}
	for {
		nLeft := uint(0)
		for range st.order {
			if st.i > len(st.order)-1 {
				st.i = 0
			}
			if len(st.events) == 0 {
				// have nothing to dispatch
				break
			}

			pid := st.order[st.i]
			demand := st.demands[pid]
			st.i++

			if demand.n == 0 || len(st.events) < int(demand.minDemand) {
				continue
			}

			item := makeDispatchItem(st.events, demand)
			demand.n--
			nLeft += demand.n
			dispatchItems = append(dispatchItems, item)
		}
		if nLeft > 0 && len(st.events) > 0 {
			continue
		}
		break
	}

	return dispatchItems
}

func makeDispatchItem(events chan etf.Term, d *demand) GenStageDispatchItem {
	item := GenStageDispatchItem{
		subscription: d.subscription,
	}

	i := uint(0)
	for {
		select {
		case e := <-events:
			item.events = append(item.events, e)
			i++
			if i == d.maxDemand {
				break
			}
			continue
		default:
			// we dont have events in the buffer
		}

		break
	}

	return item
}

func (dd *dispatcherDemand) Subscribe(subscription GenStageSubscription, opts GenStageSubscribeOptions, state interface{}) {
	st := state.(*demandState)
	newDemand := &demand{
		subscription: subscription,
		minDemand:    opts.MinDemand,
		maxDemand:    opts.MaxDemand,
	}
	st.demands[subscription.Pid] = newDemand
	st.order = append(st.order, subscription.Pid)
	return
}

// Dispatcher Broadcast implementation

func (db *dispatcherBroadcast) Init(opts GenStageOptions) interface{} {
	return nil
}

func (db *dispatcherBroadcast) Ask(subscription GenStageSubscription, count uint, state interface{}) {
	return
}

func (db *dispatcherBroadcast) Cancel(subscription GenStageSubscription, state interface{}) {
	return
}

func (db *dispatcherBroadcast) Dispatch(events etf.List, state interface{}) []GenStageDispatchItem {
	return nil
}

func (db *dispatcherBroadcast) Subscribe(subscription GenStageSubscription, opts GenStageSubscribeOptions, state interface{}) {
	return
}

// Dispatcher Partition implementation

func (dp *dispatcherPartition) Init(opts GenStageOptions) interface{} {
	return nil
}

func (dp *dispatcherPartition) Ask(subscription GenStageSubscription, count uint, state interface{}) {
	return
}

func (dp *dispatcherPartition) Cancel(subscription GenStageSubscription, state interface{}) {
	return
}

func (dp *dispatcherPartition) Dispatch(events etf.List, state interface{}) []GenStageDispatchItem {
	return nil
}

func (dp *dispatcherPartition) Subscribe(subscription GenStageSubscription, opts GenStageSubscribeOptions, state interface{}) {
	return
}
