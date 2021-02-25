package ergo

import (
	"fmt"
	"github.com/halturin/ergo/etf"
	//"github.com/halturin/ergo/lib"
)

type GenStageCancelMode uint

// GenStageOptions defines the GenStage' configuration via Init callback.
// Some options are specific to the chosen stage mode while others are
// shared across all types.
type GenStageOptions struct {

	// If this stage acts as a consumer you can to define producers
	// this stage should subscribe to.
	// subscribeTo is a list of GenStageSubscribeTo. Each element represents
	// a producer (etf.Pid or registered name) and subscription options.
	subscribeTo []GenStageSubscribeTo

	// Options below are for the stage that acts as a producer.

	// the demand is always forwarded to the HandleDemand callback.
	// When this options is set to 'true', demands are accumulated until mode is
	// set back to 'false' via DisableDemandAccumulating method
	disableForwarding bool

	// bufferSize the size of the buffer to store events without demand.
	// default is 10000
	bufferSize uint

	// bufferKeepLast defines whether the first or last entries should be
	// kept on the buffer in case the buffer size is exceeded.
	bufferKeepLast bool

	dispatcher GenStageDispatcherBehaviour
}

const (
	GenStageCancelPermanent GenStageCancelMode = 0
	GenStageCancelTransient GenStageCancelMode = 1
	GenStageCancelTemporary GenStageCancelMode = 2
)

var (
	ErrNotAProducer = fmt.Errorf("not a producer")
)

type GenStageBehaviour interface {

	// InitStage
	InitStage(process *Process, args ...interface{}) (GenStageOptions, interface{})

	// HandleDemand this callback is invoked on a producer stage
	// The producer that implements this callback must either store the demand, or return the amount of requested events.
	// Use `ergo.ErrStop` as an error for the normal shutdown this process. Any other error values
	// will be used as a reason for the abnornal shutdown process.
	HandleDemand(subscription GenStageSubscription, demand uint, state interface{}) (error, etf.List, interface{})

	// HandleEvents this callback is invoked on a consumer stage.
	// Use `ergo.ErrStop` as an error for the normal shutdown this process. Any other error values
	// will be used as a reason for the abnornal shutdown process.
	HandleEvents(subscription GenStageSubscription, events etf.List, state interface{}) (error, interface{})

	// HandleSubscribe This callback is invoked on a producer stage.
	// Use `ergo.ErrStop` as an error for the normal shutdown this process. Any other error values
	// will be used as a reason for the abnornal shutdown process.
	HandleSubscribe(subscription GenStageSubscription, options GenStageSubscribeOptions, state interface{}) (error, interface{})

	// HandleSubscribed this callback is invoked as a confirmation for the subscription request
	// Successful subscription must return one of:
	//   - (GenStageSubscriptionModeAuto, state)
	//      means the stage implementation will take care of automatically sending
	//      demand to producers. This is the default
	//   - (GenStageSubscriptionModeManual, state)
	//     means that demand must be sent to producers explicitly via Ask method.
	//     this kind of subscription must be canceled when HandleCancel is called
	HandleSubscribed(subscription GenStageSubscription, state interface{}) (error, bool, interface{})

	// HandleCancel
	// Invoked when a consumer is no longer subscribed to a producer (invoked on a producer stage)
	// The cancelReason will be a {Cancel: "cancel", Reason: _} if the reason for cancellation
	// was a GenStage.Cancel call. Any other value means the cancellation reason was
	// due to an EXIT.
	// Use `ergo.ErrStop` as an error for the normal shutdown this process. Any other error values
	// will be used as a reason for the abnornal shutdown process.
	HandleCancel(subscription GenStageSubscription, reason string, state interface{}) (error, interface{})

	// HandleCanceled
	// Invoked when a consumer is no longer subscribed to a producer (invoked on a consumer stage)
	// Termination this stage depends on a cancel mode for the given subscription. For the cancel mode
	// GenStageCancelPermanent - this stage will be terminated right after this callback invoking.
	// For the cancel mode GenStageCancelTransient - it depends on a reason of subscription canceling.
	// Cancel mode GenStageCancelTemporary keeps this stage alive whether the reason could be.
	HandleCanceled(subscription GenStageSubscription, reason string, state interface{}) (error, interface{})

	// HandleGenStageCall this callback is invoked on Process.Call. This method is optional
	// for the implementation
	HandleGenStageCall(from etf.Tuple, message etf.Term, state interface{}) (string, etf.Term, interface{})
	// HandleGenStageCast this callback is invoked on Process.Cast. This method is optional
	// for the implementation
	HandleGenStageCast(message etf.Term, state interface{}) (string, interface{})
	// HandleGenStageInfo this callback is invoked on Process.Send. This method is optional
	// for the implementation
	HandleGenStageInfo(message etf.Term, state interface{}) (string, interface{})
}

type GenStageSubscription struct {
	Pid etf.Pid
	Ref etf.Ref
}

type subscriptionInternal struct {
	Ref     etf.Ref
	Monitor etf.Ref
}

type GenStageSubscribeTo struct {
	producer etf.Term
	options  GenStageSubscribeOptions
}

type GenStageSubscribeOptions struct {
	MinDemand uint `etf:"min_demand"`
	MaxDemand uint `etf:"max_demand"`
	// The stage implementation will take care of automatically sending
	// demand to producer (as a default behaviour). You can disable it
	// setting ManualDemand to true
	ManualDemand bool `etf:"mode"`
	// What should happend with consumer if producer has terminated
	// GenStageCancelPermanent the consumer exits when the producer cancels or exits.
	// GenStageCancelTransient the consumer exits only if reason is not "normal",
	// "shutdown", or {"shutdown", _}
	// GenStageCancelTemporary the consumer never exits
	Cancel GenStageCancelMode `etf:"cancel"`
}

type GenStageCancelReason struct {
	Cancel string
	Reason string
}

type GenStage struct {
	GenServer
}

type stateGenStage struct {
	p               *Process
	internal        interface{}
	options         GenStageOptions
	demandBuffer    []demandRequest
	dispatcherState interface{}
	// keep our subscriptions
	// key: string representation of etf.Ref (monitor)
	producers map[string]subscriptionInternal
	// keep our subscribers
	consumers map[etf.Pid]subscriptionInternal
}

type stageRequestCommandCancel struct {
	Subscription etf.Ref
	Reason       etf.Term
}

type stageRequestCommand struct {
	Cmd  etf.Atom
	Opt1 interface{}
	Opt2 interface{}
}

type stageMessage struct {
	Request      etf.Atom
	Subscription GenStageSubscription
	Command      stageRequestCommand
}

type downMessage struct {
	Down   etf.Atom // = etf.Atom("DOWN")
	Ref    etf.Ref  // a monitor reference
	Type   etf.Atom // = etf.Atom("process")
	From   etf.Term // Pid or Name. Depends on how Monitor was called - by name or by pid
	Reason string
}

type setManualDemand struct {
	subscription GenStageSubscription
	enable       bool
}

type setCancelMode struct {
	subscription GenStageSubscription
	mode         GenStageCancelMode
}

type doSubscribe struct {
	to      etf.Term
	options GenStageSubscribeOptions
}

type setForwardDemand struct {
	forward bool
}

type demandRequest struct {
	subscription GenStageSubscription
	demand       uint
}

type sendEvents struct {
	events etf.List
}

// GenStage methods

// DisableAutoDemand means that demand must be sent to producers explicitly via Ask method. This
// mode can be used when a special behaviour is desired.
func (gst *GenStage) DisableAutoDemand(p *Process, subscription GenStageSubscription) error {
	message := setManualDemand{
		subscription: subscription,
		enable:       false,
	}
	_, err := p.Call(p.Self(), message)
	return err
}

// EnableAutoDemand enables auto demand mode (this is default mode for the consumer).
func (gst *GenStage) EnableAutoDemand(p *Process, subscription GenStageSubscription) error {
	if p == nil {
		return fmt.Errorf("Subscription error. Process can not be nil")
	}
	message := setManualDemand{
		subscription: subscription,
		enable:       false,
	}
	_, err := p.Call(p.Self(), message)
	return err
}

// EnableForwardDemand enables forwarding messages to the HandleDemand on a producer stage.
// This is default mode for the producer.
func (gst *GenStage) EnableForwardDemand(p *Process) error {
	message := setForwardDemand{
		forward: true,
	}
	_, err := p.Call(p.Self(), message)
	return err
}

// DisableForwardDemand disables forwarding messages to the HandleDemand on a producer stage.
// This is useful as a synchronization mechanism, where the demand is accumulated until
// all consumers are subscribed.
func (gst *GenStage) DisableForwardDemand(p *Process) error {
	message := setForwardDemand{
		forward: false,
	}
	_, err := p.Call(p.Self(), message)
	return err
}

// SendEvents sends events for the subscribers
func (gst *GenStage) SendEvents(p *Process, events etf.List) error {
	message := sendEvents{
		events: events,
	}
	_, err := p.Call(p.Self(), message)
	return err
}

// SetCancelMode defines how consumer will handle termination of the producer. There are 3 modes:
// GenStageCancelPermanent (default) - consumer exits when the producer cancels or exits
// GenStageCancelTransient - consumer exits only if reason is not normal, shutdown, or {shutdown, reason}
// GenStageCancelTemporary - never exits
func (gst *GenStage) SetCancelMode(p *Process, subscription GenStageSubscription, cancel GenStageCancelMode) {
	message := setCancelMode{
		subscription: subscription,
		mode:         cancel,
	}
	p.Call(p.Self(), message)
	return
}

// Subscribe subscribes to the given producer. HandleSubscribed callback will be invoked on a consumer stage once a request for the subscription is sent. If something went wrong on a producer side the callback HandleCancel will be invoked with a reason of cancelation.
func (gst *GenStage) Subscribe(p *Process, producer etf.Term, opts GenStageSubscribeOptions) GenStageSubscription {
	var subscription GenStageSubscription

	subscription_id := p.MonitorProcess(producer)
	subscription.Pid = p.Self()
	subscription.Ref = subscription_id

	subscribe_opts := etf.List{
		etf.Tuple{
			etf.Atom("min_demand"),
			opts.MinDemand,
		},
		etf.Tuple{
			etf.Atom("max_demand"),
			opts.MaxDemand,
		},
		etf.Tuple{
			etf.Atom("cancel"),
			opts.Cancel,
		},
	}

	// In order to get rid of race condition we should send this message
	// before we send 'subscribe' to the producer process. Just
	// to make sure if we registered this subscription before the 'DOWN'
	// or 'EXIT' message arrived in case of something went wrong.
	msg := etf.Tuple{
		etf.Atom("$gen_consumer"),
		etf.Tuple{p.Self(), subscription_id},
		etf.Tuple{etf.Atom("subscribed"), producer, subscribe_opts},
	}
	p.Send(p.Self(), msg)

	msg = etf.Tuple{
		etf.Atom("$gen_producer"),
		etf.Tuple{p.Self(), subscription_id},
		etf.Tuple{etf.Atom("subscribe"), nil, subscribe_opts},
	}
	p.Send(producer, msg)

	return subscription
}

// Ask makes a demand request for the given subscription. This function must only be
// used in the cases when a consumer sets a subscription to manual mode via DisableAutoDemand
func (gst *GenStage) Ask(p *Process, subscription GenStageSubscription, demand uint) {
	if demand == 0 {
		return
	}

	return
}

// Cancel
func (gst *GenStage) Cancel(p *Process, subscription GenStageSubscription, reason string) {
	msg := etf.Tuple{
		etf.Atom("$gen_consumer"),
		etf.Tuple{subscription.Pid, subscription.Ref},
		etf.Tuple{etf.Atom("cancel"), reason},
	}
	p.Send(subscription.Pid, msg)
	return
}

//
// GenServer callbacks
//
func (gs *GenStage) Init(p *Process, args ...interface{}) interface{} {
	//var stageOptions GenStageOptions
	state := &stateGenStage{
		producers: map[string]subscriptionInternal{},
		consumers: map[etf.Pid]subscriptionInternal{},
	}

	state.p = p
	state.options, state.internal = p.object.(GenStageBehaviour).InitStage(p, args)

	// if dispatcher wasn't specified create a default one GenStageDispatcherDemand
	if state.options.dispatcher == nil {
		state.options.dispatcher = CreateGenStageDispatcher(GenStageDispatcherDemand)
	}

	state.dispatcherState = state.options.dispatcher.Init(state.options)

	return state
}

func (gs *GenStage) HandleCall(from etf.Tuple, message etf.Term, state interface{}) (string, etf.Term, interface{}) {
	fmt.Println("Stage call")
	st := state.(*stateGenStage)

	switch m := message.(type) {
	case setManualDemand:
		// m.enable
		fmt.Println("setManualDemand", m)
		return "reply", "ok", state

	case setCancelMode:
		// m.cancel
		return "reply", "ok", state

	case setForwardDemand:
		// m.forward
		// disableForwarding = !m.forward
		return "reply", "ok", state
	case sendEvents:
		// m.events
		// dispatch to the subscribers
		return "reply", "ok", state

	default:
		reply, term, internal := st.p.object.(GenStageBehaviour).HandleGenStageCall(from, message, st.internal)
		st.internal = internal
		return reply, term, state
	}

	return "reply", "ok", state
}

func (gs *GenStage) HandleCast(message etf.Term, state interface{}) (string, interface{}) {
	fmt.Println("Stage cast")

	st := state.(*stateGenStage)
	reply, internal := st.p.object.(GenStageBehaviour).HandleGenStageCast(message, st.internal)
	st.internal = internal

	return reply, state
}

func (gs *GenStage) HandleInfo(message etf.Term, state interface{}) (string, interface{}) {
	var r stageMessage
	var d downMessage
	var err error

	fmt.Println("Stage info")

	st := state.(*stateGenStage)

	// check if we got a 'DOWN' mesaage
	// {DOWN, Ref, process, PidOrName, Reason}
	if err := etf.TermIntoStruct(message, &d); err == nil && d.Down == etf.Atom("DOWN") {

		// send Cancel message to itself
		return "noreply", state
	}

	if err := etf.TermIntoStruct(message, &r); err != nil {
		reply, internal := st.p.object.(GenStageBehaviour).HandleGenStageInfo(message, st.internal)
		st.internal = internal
		return reply, state
	}

	_, err = handleRequest(r, st)

	switch err {
	case nil:
		return "noreply", state
	case ErrStop:
		return "stop", "normal"
	case ErrUnsupportedRequest:
		reply, internal := st.p.object.(GenStageBehaviour).HandleGenStageInfo(message, st.internal)
		st.internal = internal
		return reply, state
	default:
		return "stop", err.Error()
	}

	return "noreply", state
}

func (gs *GenStage) Terminate(reason string, state interface{}) {
	fmt.Println("Stage terminate")
	return
}

// default callbacks

func (gs *GenStage) InitStage(process *Process, args ...interface{}) (GenStageOptions, interface{}) {
	// GenStage initialization with default options
	opts := GenStageOptions{}
	return opts, nil
}

func (gs *GenStage) HandleGenStageCall(from etf.Tuple, message etf.Term, state interface{}) (string, etf.Term, interface{}) {
	// default callback if it wasn't implemented
	fmt.Printf("HandleGenStageCall: unhandled message (from %#v) %#v\n", from, message)
	return "reply", etf.Atom("ok"), state
}

func (gs *GenStage) HandleGenStageCast(message etf.Term, state interface{}) (string, interface{}) {
	// default callback if it wasn't implemented
	fmt.Printf("HandleGenStageCast: unhandled message %#v\n", message)
	return "noreply", state
}
func (gs *GenStage) HandleGenStageInfo(message etf.Term, state interface{}) (string, interface{}) {
	// default callback if it wasn't implemnted
	fmt.Printf("HandleGenStageInfo: unhandled message %#v\n", message)
	return "noreply", state
}

func (gs *GenStage) HandleSubscribe(subscription GenStageSubscription, options GenStageSubscribeOptions,
	state interface{}) (error, interface{}) {
	return ErrNotAProducer, state
}

func (gs *GenStage) HandleSubscribed(subscription GenStageSubscription, state interface{}) (error, bool, interface{}) {
	// set default subscription mode to GenStageSubscriptionModeAuto
	// FIXME get this value from the state and return it
	return nil, false, state
}

func (gs *GenStage) HandleCancel(subscription GenStageSubscription, reason string, state interface{}) (error, interface{}) {
	// default callback if it wasn't implemented
	return nil, state
}

func (gs *GenStage) HandleCanceled(subscription GenStageSubscription, reason string, state interface{}) (error, interface{}) {
	// default callback if it wasn't implemented
	return nil, state
}

func (gs *GenStage) HandleEvents(subscription GenStageSubscription, events etf.List, state interface{}) (error, interface{}) {
	fmt.Printf("GenStage HandleEvents: unhandled subscription (%#v) events %#v\n", subscription, events)
	return nil, state
}

func (gs *GenStage) HandleDemand(subscription GenStageSubscription, demand uint, state interface{}) (error, etf.List, interface{}) {
	fmt.Printf("GenStage HandleDemand: unhandled subscription (%#v) demand %#v\n", subscription, demand)
	return nil, nil, state
}

// private functions

func handleRequest(m stageMessage, state *stateGenStage) (etf.Term, error) {
	switch m.Request {
	case "$gen_consumer":
		return handleConsumer(m.Subscription, m.Command, state)
	case "$gen_producer":
		return handleProducer(m.Subscription, m.Command, state)
	}
	return nil, ErrUnsupportedRequest
}

func handleConsumer(subscription GenStageSubscription, cmd stageRequestCommand, state *stateGenStage) (etf.Term, error) {
	fmt.Printf("handleConsumer %#v\n", cmd)
	var subscriptionOpts GenStageSubscribeOptions
	var err error
	var manualDemand bool

	switch cmd.Cmd {
	case etf.Atom("events"):
		events := cmd.Opt1.(etf.List)
		object := state.p.object
		err, state.internal = object.(GenStageBehaviour).HandleEvents(subscription, events, state.internal)
		if err != nil {
			return nil, err
		}
		return etf.Atom("ok"), nil

	case etf.Atom("subscribed"):
		if err := etf.TermProplistIntoStruct(cmd.Opt2, &subscriptionOpts); err != nil {
			return nil, err
		}

		object := state.p.object
		err, manualDemand, state.internal = object.(GenStageBehaviour).HandleSubscribed(subscription, state.internal)

		if err != nil {
			return nil, err
		}
		// cmd.Opt1 - producer
		//state.producers[cmd.Opt1]

		if !manualDemand {
			// call Ask method
		}

		return etf.Atom("ok"), nil

	case etf.Atom("cancel"):
		reason, ok := cmd.Opt1.(string)
		if !ok {
			return nil, fmt.Errorf("Cancel reason not a string")
		}

		object := state.p.object
		err, state.internal = object.(GenStageBehaviour).HandleCanceled(subscription, reason, state.internal)
		if err != nil {
			return nil, err
		}

		return etf.Atom("ok"), nil
	}

	return nil, fmt.Errorf("unknown GenStage command (HandleCast)")
}

func handleProducer(subscription GenStageSubscription, cmd stageRequestCommand, state *stateGenStage) (etf.Term, error) {
	fmt.Printf("handleProducer %#v\n", cmd)
	var subscriptionOpts GenStageSubscribeOptions
	var err error

	switch cmd.Cmd {
	case etf.Atom("subscribe"):

		// {subscribe, Cancel, Opts}
		if err = etf.TermProplistIntoStruct(cmd.Opt2, &subscriptionOpts); err != nil {
			return nil, err
		}

		object := state.p.object
		err, state.internal = object.(GenStageBehaviour).HandleSubscribe(subscription, subscriptionOpts, state.internal)

		switch err {
		case nil:
			// cancel current subscription if this consumer has been already subscribed
			if s, ok := state.consumers[subscription.Pid]; ok {
				msg := etf.Tuple{
					etf.Atom("$gen_consumer"),
					etf.Tuple{subscription.Pid, s.Ref},
					etf.Tuple{etf.Atom("cancel"), "resubscribed"},
				}
				state.p.Send(subscription.Pid, msg)
				// notify dispatcher about cancelation the previous subscription
				canceledSubscription := GenStageSubscription{
					Pid: subscription.Pid,
					Ref: s.Ref,
				}
				// cancel current demands
				state.dispatcherState = state.options.dispatcher.Cancel(canceledSubscription, state.dispatcherState)
				// notify dispatcher about the new subscription
				state.dispatcherState = state.options.dispatcher.Subscribe(subscription, subscriptionOpts, state.dispatcherState)

				s.Ref = subscription.Ref
				state.consumers[subscription.Pid] = s
				return etf.Atom("ok"), nil
			}

			// monitor subscriber in order to remove this subscription
			// if it terminated unexpectedly
			m := state.p.MonitorProcess(subscription.Pid)
			s := subscriptionInternal{
				Ref:     subscription.Ref,
				Monitor: m,
			}
			state.consumers[subscription.Pid] = s
			state.dispatcherState = state.options.dispatcher.Subscribe(subscription, subscriptionOpts, state.dispatcherState)
			return etf.Atom("ok"), nil

		case ErrNotAProducer:
			// if it wasnt overloaded - send 'cancel' to the consumer
			msg := etf.Tuple{
				etf.Atom("$gen_consumer"),
				etf.Tuple{subscription.Pid, subscription.Ref},
				etf.Tuple{etf.Atom("cancel"), "not a producer"},
			}
			state.p.Send(subscription.Pid, msg)
			return etf.Atom("ok"), nil

		default:
			// any other error should terminate this stage
			return nil, err
		}

	case etf.Atom("ask"):
		var events etf.List
		var deliver []GenStageDispatchItem
		// {ask, Demand}
		demand, ok := cmd.Opt1.(uint)
		if !ok {
			return nil, fmt.Errorf("Demand has wrong value. Expected uint")
		}

		if state.options.disableForwarding {
			d := demandRequest{
				subscription: subscription,
				demand:       demand,
			}
			// FIXME it would be more effective to use sync.Pool with
			// preallocated array behind the slice.
			// see how it was made in lib.TakeBuffer
			state.demandBuffer = append(state.demandBuffer, d)
			return etf.Atom("ok"), nil
		}

		// send cancel if we got demand from a consumer which hasn't subscription
		if _, ok := state.consumers[subscription.Pid]; !ok {
			msg := etf.Tuple{
				etf.Atom("$gen_consumer"),
				etf.Tuple{subscription.Pid, subscription.Ref},
				etf.Tuple{etf.Atom("cancel"), "not subscribed"},
			}
			state.p.Send(subscription.Pid, msg)
			return etf.Atom("ok"), nil
		}

		object := state.p.object
		_, events, state.internal = object.(GenStageBehaviour).HandleDemand(subscription, demand, state.internal)

		// register this demand in the dispatcher
		dispatcher := state.options.dispatcher
		state.dispatcherState = dispatcher.Ask(subscription, demand, state.dispatcherState)
		// if HandleDemand provided events for the dispatching handle it right away
		if len(events) == 0 {
			return etf.Atom("ok"), nil
		}

		deliver, state.dispatcherState = dispatcher.Dispatch(events, state.dispatcherState)
		if len(deliver) == 0 {
			return etf.Atom("ok"), nil
		}

		for d := range deliver {
			msg := etf.Tuple{
				etf.Atom("$gen_consumer"),
				etf.Tuple{deliver[d].subscription.Pid, deliver[d].subscription.Ref},
				deliver[d].events,
			}
			state.p.Send(deliver[d].subscription.Pid, msg)
		}

		return etf.Atom("ok"), nil

	case etf.Atom("cancel"):
		var e error
		// handle this cancelation in the dispatcher
		dispatcher := state.options.dispatcher
		state.dispatcherState = dispatcher.Cancel(subscription, state.dispatcherState)
		// handle it in a GenStage callback
		object := state.p.object
		reason := cmd.Opt1.(string)
		e, state.internal = object.(GenStageBehaviour).HandleCancel(subscription, reason, state.internal)
		return etf.Atom("ok"), e
	}

	return nil, fmt.Errorf("unknown GenStage command (HandleCall)")
}
