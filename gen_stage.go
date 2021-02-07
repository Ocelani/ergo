package ergo

import (
	"fmt"
	"github.com/halturin/ergo/etf"
	//"github.com/halturin/ergo/lib"
)

type GenStageSubscriptionMode string
type GenStageSubscriptionCancel string
type GenStageDemandMode string
type GenStageType int

// GenStageOptions defines the GenStage' configuration via Init callback.
// Some options are specific to the chosen stage mode while others are
// shared across all types.
type GenStageOptions struct {
	// mode defines the way of acting
	stageType GenStageType

	// ---- options for GenStageModeProducer

	// when set to GenStageDemandModeForward, the demand is always forwarded
	// to the HandleDemand callback. When this options is set to
	// GenStageDemandModeAccumulate, demands are accumulated until mode is
	// set back to GenStageDemandModeForward via Demand method
	demand GenStageDemandMode

	// ---- options for GenStageModeProducer and GenStageModeProducerConsumer

	// bufferSize the size of the buffer to store events without demand.
	// default is 10000
	bufferSize uint

	// bufferKeepFirst defines whether the first or last entries should be
	// kept on the buffer in case the buffer size is exceeded.
	bufferKeepFirst bool

	// ---- options for GenStageModeConsumer and GenStageModeProducerConsumer

	// subscribeTo a list of producers to subscribe to. Each element represents
	// a tuple with process of Producer (atom, tuple with registered name and node,
	// etf.Pid) and the subscription options.
	subscribeTo []etf.Term

	dispatcher      GenStageDispatcherBehaviour
	dispatcherState interface{}
}

const (
	// GenStageSubscriptionModeAuto means the stage implementation will take care
	// of automatically sending demand to producers. This is the default
	GenStageSubscriptionModeAuto GenStageSubscriptionMode = "automatic"
	// GenStageSubscriptionModeManual means that demand must be sent to producers
	// explicitly via GenServer.Ask
	GenStageSubscriptionModeManual GenStageSubscriptionMode = "manual"
	GenStageSubscriptionModeStop   GenStageSubscriptionMode = "stop"

	// GenStageSubscriptionCancelPermanent the consumer exits when the producer cancels or exits.
	GenStageSubscriptionCancelPermanent GenStageSubscriptionCancel = "permanent"
	// GenStageSubscriptionCancelTransient the consumer exits only if reason is not "normal",
	// "shutdown", or {"shutdown", _}
	GenStageSubscriptionCancelTransient GenStageSubscriptionCancel = "transient"
	// GenStageSubscriptionCancelTemporary the consumer never exits
	GenStageSubscriptionCancelTemporary GenStageSubscriptionCancel = "temporary"

	GenStageDemandModeForward    GenStageDemandMode = "forward"
	GenStageDemandModeAccumulate GenStageDemandMode = "accumulate"

	GenStageTypeProducer         GenStageType = 1
	GenStageTypeConsumer         GenStageType = 2
	GenStageTypeProducerConsumer GenStageType = 3
)

type GenStageBehaviour interface {

	// InitStage(...) -> (GenStageOptions, state)
	InitStage(process *Process, args ...interface{}) (GenStageOptions, interface{})

	// HandleCancel
	// Invoked when a consumer is no longer subscribed to a producer.
	// HandleCancel -> ("noreply", subscription, state)
	//                 ("stop", reason, _)
	// where cancelReason is
	// 				etf.Tuple{"cancel", etf.Term(reason)}
	//              etf.Tuple{"down", etf.Term(reason)}
	// The cancelReason will be a {"cancel", _} tuple if the reason for cancellation
	// was a GenStage.cancel call. Any other value means the cancellation reason was
	// due to an EXIT.
	// Return values are the same as HandleCast.
	HandleCancel(subscription GenStageSubscription, cancelReason etf.Term, state interface{}) (string, etf.Term, interface{})

	// HandleDemand
	// HandleDemand -> ("noreply", event, state)
	//                 ("stop", reason, _)
	// invoked on GenStageTypeProducer.
	// The producer that implements this callback must either store the demand, or return the amount of requested events.
	HandleDemand(subscription GenStageSubscription, demand uint, state interface{}) (string, []etf.Term, interface{})

	// HandleEvents
	HandleEvents(subscription GenStageSubscription, events []etf.Term, state interface{}) (string, interface{})

	// HandleSubscribe This callback is invoked in both producers and consumers.
	// stageType will be GenStageTypeProducer if this callback is invoked on a consumer
	// and GenStageTypeConsumer if when this callback is invoked on producers a consumer subscribed to.
	// For consumers,  successful subscriptions must return one of:
	//   - (GenStageSubscriptionModeAuto, state)
	//      means the stage implementation will take care of automatically sending
	//      demand to producers. This is the default
	//   - (GenStageSubscriptionModeManual, state)
	//     means that demand must be sent to producers explicitly via Ask method.
	//     this kind of subscription must be canceled when HandleCancel is called
	// For producers, successful subscriptions must always return GenStageSubscriptionAuto.
	// Manual mode is not supported.
	HandleSubscribe(stageType GenStageType, subscription GenStageSubscription, options GenStageSubscriptionOptions, state interface{}) (GenStageSubscriptionMode, interface{})
}

type GenStageSubscription struct {
	Pid etf.Pid
	Ref etf.Ref
}

type GenStageSubscriptionOptions struct {
	MinDemand uint                       `etf:"min_demand"`
	MaxDemand uint                       `etf:"max_demand"`
	Mode      GenStageSubscriptionMode   `etf:"mode"`
	Cancel    GenStageSubscriptionCancel `etf:"cancel"`
}

type GenStage struct {
	GenServer
}

type stateGenStage struct {
	p        *Process
	internal interface{}
	options  GenStageOptions
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

//
// GenStage' object methods
//

// SetDemandMode Sets the demand mode for a producer
// When DemandModeForward (by default), the demand is always forwarded to the HandleDemand callback.
// When DemandModeAccumulate, demand is accumulated until its mode is set to DemandModeForward.
// This is useful as a synchronization mechanism, where the demand is accumulated until
// all consumers are subscribed.
func (gst *GenStage) SetDemandMode(p *Process, mode GenStageDemandMode) error {
	_, err := p.Call(p.Self(), etf.Atom("$set_demand_mode"))
	return err
}
func (gst *GenStage) GetDemandMode(p *Process) (GenStageDemandMode, error) {
	mode, err := p.Call(p.Self(), etf.Atom("$get_demand_mode"))
	if err != nil {
		return GenStageDemandModeForward, err
	}
	return mode.(GenStageDemandMode), nil
}

func (gst *GenStage) Subscribe(p *Process, to etf.Term, opts GenStageSubscriptionOptions) (GenStageSubscription, error) {
	var subscription GenStageSubscription
	if p == nil {
		return subscription, fmt.Errorf("Subscription error. Process can not be nil")
	}
	if !p.IsAlive() {
		return subscription, fmt.Errorf("Subscription error. Process should be alive")
	}

	subscription_id := p.MonitorProcess(to)
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

	msg := etf.Tuple{
		etf.Atom("$gen_producer"),
		etf.Tuple{p.Self(), subscription_id},
		etf.Tuple{etf.Atom("subscribe"), nil, subscribe_opts},
	}
	if _, err := p.Call(to, msg); err != nil {
		return subscription, err
	}

	msg[0] = etf.Atom("$gen_consumer")
	p.Cast(p.Self(), msg)

	return subscription, nil
}

func (gst *GenStage) Ask(subscription GenStageSubscription, demand uint) error {
	if demand == 0 {
		return nil
	}
	return nil
}

func (gst *GenStage) Cancel(subscription etf.Ref) error {
	return nil
}

//
// GenServer callbacks
//
func (gs *GenStage) Init(p *Process, args ...interface{}) interface{} {
	//var stageOptions GenStageOptions
	var state stateGenStage

	state.p = p
	state.options, state.internal = p.object.(GenStageBehaviour).InitStage(p, args)

	if state.options.stageType == GenStageTypeConsumer {
		return state
	}

	// if dispatcher wasn't specified create a default one GenStageDispatcherDemand
	if state.options.dispatcher == nil {
		state.options.dispatcher = CreateGenStageDispatcher(GenStageDispatcherDemand)
	}

	state.options.dispatcherState = state.options.dispatcher.Init(state.options)

	return state
}

func (gs *GenStage) HandleCall(from etf.Tuple, message etf.Term, state interface{}) (string, etf.Term, interface{}) {
	var r stageMessage
	fmt.Println("Stage call")
	if err := etf.TermIntoStruct(message, &r); err != nil {
		fmt.Println("Stage call err", err)
		return "reply", "error", state
	}

	if err, _ := handleRequest(r, state.(stateGenStage)); err != nil {
		return "reply", "error", state
	}

	return "reply", "ok", state
}

func (gs *GenStage) HandleCast(message etf.Term, state interface{}) (string, interface{}) {
	fmt.Println("Stage cast")
	var r stageMessage
	if err := etf.TermIntoStruct(message, &r); err != nil {
		fmt.Println("Stage cast err", err)
		return "noreply", state
	}

	if err, _ := handleRequest(r, state.(stateGenStage)); err != nil {
		fmt.Println("Stage cast err handleRequest", err)
		return "noreply", state
	}
	return "noreply", state
}

func (gs *GenStage) HandleInfo(message etf.Term, state interface{}) (string, interface{}) {
	fmt.Println("Stage info")
	return "noreply", state
}

func (gs *GenStage) Terminate(reason string, state interface{}) {
	fmt.Println("Stage term")
	return
}

// private functions

func handleRequest(m stageMessage, state stateGenStage) (error, stateGenStage) {
	switch m.Request {
	case "$gen_consumer":
		return handleConsumer(m.Subscription, m.Command, state)
	case "$gen_producer":
		return handleProducer(m.Subscription, m.Command, state)
	}

	return fmt.Errorf("unknownRequest"), state
}

func handleConsumer(subscription GenStageSubscription, cmd stageRequestCommand, state stateGenStage) (error, stateGenStage) {
	fmt.Printf("handleConsumer %#v\n", cmd)
	var subscriptionOpts GenStageSubscriptionOptions
	var subscriptionMode GenStageSubscriptionMode
	switch cmd.Cmd {
	case etf.Atom("subscribe"):
		// receive this message as a confirmation of subscription
		if state.options.stageType == GenStageTypeProducer {
			return fmt.Errorf("GenStage with type GenStageTypeProducer cannot act as a consumer stage"), state
		}
		object := state.p.object

		if err := etf.TermProplistIntoStruct(cmd.Opt2, &subscriptionOpts); err != nil {
			return err, state
		}
		subscriptionMode, state.internal = object.(GenStageBehaviour).HandleSubscribe(GenStageTypeProducer, subscription, subscriptionOpts, state.internal)

		switch subscriptionMode {
		case GenStageSubscriptionModeStop:
			// FIXME have to stop this process
		case GenStageSubscriptionModeAuto:
			// call Ask method

		case GenStageSubscriptionModeManual:

		}
		return nil, state
	}
	return nil, state
}

func handleProducer(subscription GenStageSubscription, cmd stageRequestCommand, state stateGenStage) (error, stateGenStage) {
	fmt.Printf("handleProducer %#v\n", cmd)
	var subscriptionOpts GenStageSubscriptionOptions
	var subscriptionMode GenStageSubscriptionMode
	var events []etf.Term
	if state.options.stageType == GenStageTypeConsumer {
		return fmt.Errorf("GenStage with type GenStageTypeConsumer cannot act as a producer stage"), state
	}
	switch cmd.Cmd {
	case etf.Atom("subscribe"):
		// {subscribe, Cancel, Opts}
		if err := etf.TermProplistIntoStruct(cmd.Opt2, &subscriptionOpts); err != nil {
			return err, state
		}
		object := state.p.object
		subscriptionMode, state.internal = object.(GenStageBehaviour).HandleSubscribe(GenStageTypeConsumer, subscription, subscriptionOpts, state.internal)

		switch subscriptionMode {
		case GenStageSubscriptionModeStop:
			//FIXME have to stop this process
		case GenStageSubscriptionModeAuto:
			// For producers, successful subscriptions must always return GenStageSubscriptionAuto

			// Process.send(pid, {:"$gen_producer", {self(), ref}, {:ask, demand}}, opts)
		default:

		}

		return nil, state

	case etf.Atom("ask"):
		// {ask, Demand}
		demand, ok := cmd.Opt1.(uint)
		if !ok {
			return fmt.Errorf("Demand has wrong value. Expected uint"), state
		}
		object := state.p.object
		_, events, state.internal = object.(GenStageBehaviour).HandleDemand(subscription, demand, state.internal)
		// FIXME handle events

		dispatcher := state.options.dispatcher
		dispatcherState := state.options.dispatcherState
		state.options.dispatcherState = dispatcher.Ask(subscription, demand, dispatcherState)

		fmt.Println("GOT DEMAND", demand, events)

	case etf.Atom("cancel"):
		// {cancel, Reason}
		// <Object>.HandleCancel(cmd.Opts, from, state)
	}
	return nil, state
}
