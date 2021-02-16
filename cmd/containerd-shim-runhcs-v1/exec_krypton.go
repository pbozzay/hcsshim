package main

import (
	"context"
	"sync"
	"time"

	"github.com/Microsoft/hcsshim/internal/cmd"
	"github.com/Microsoft/hcsshim/internal/cow"
	"github.com/Microsoft/hcsshim/internal/guestrequest"
	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/signals"
	"github.com/Microsoft/hcsshim/osversion"
	eventstypes "github.com/containerd/containerd/api/events"
	containerd_v1_types "github.com/containerd/containerd/api/types/task"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/runtime"
	"github.com/containerd/containerd/runtime/v2/task"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.opencensus.io/trace"
)

// newKryptonExec creates an exec to track the lifetime of `spec` in `c` which is
// actually created on the call to `Start()`. If `id==tid` then this is the init
// exec and the exec will also start `c` on the call to `Start()` before execing
// the process `spec.Process`.
func newKryptonExec(
	ctx context.Context,
	events publisher,
	tid string,
	c cow.Container,
	id, bundle string,
	isWCOW bool,
	spec *specs.Process,
	io cmd.UpstreamIO) shimExec {
	log.G(ctx).WithFields(logrus.Fields{
		"tid":    tid,
		"eid":    id, // Init exec ID is always same as Task ID
		"bundle": bundle,
		"wcow":   isWCOW,
	}).Debug("newKryptonExec")

	ke := &kryptonExec{
		events:      events,
		tid:         tid,
		c:           c,
		id:          id,
		bundle:      bundle,
		isWCOW:      isWCOW,
		spec:        spec,
		io:          io,
		processDone: make(chan struct{}),
		state:       shimExecStateCreated,
		exitStatus:  255, // By design for non-exited process status.
		exited:      make(chan struct{}),
	}
	go ke.waitForContainerExit()
	return ke
}

var _ = (shimExec)(&kryptonExec{})

type kryptonExec struct {
	events publisher
	// tid is the task id of the container hosting this process.
	//
	// This MUST be treated as read only in the lifetime of the exec.
	tid string
	// host is the hosting VM for `c`. If `host==nil` this exec MUST be a
	// process isolated WCOW exec.
	//
	// This MUST be treated as read only in the lifetime of the exec.
	// host *uvm.UtilityVM
	// c is the hosting container for this exec.
	//
	// This MUST be treated as read only in the lifetime of the exec.
	c cow.Container
	// id is the id of this process.
	//
	// This MUST be treated as read only in the lifetime of the exec.
	id string
	// bundle is the on disk path to the folder containing the `process.json`
	// describing this process. If `id==tid` the process is described in the
	// `config.json`.
	//
	// This MUST be treated as read only in the lifetime of the exec.
	bundle string
	// isWCOW is set to `true` when this process is part of a Windows OCI spec.
	//
	// This MUST be treated as read only in the lifetime of the exec.
	isWCOW bool
	// spec is the OCI Process spec that was passed in at create time. This is
	// stored because we don't actually create the process until the call to
	// `Start`.
	//
	// This MUST be treated as read only in the lifetime of the exec.
	spec *specs.Process
	// io is the upstream io connections used for copying between the upstream
	// io and the downstream io. The upstream IO MUST already be connected at
	// create time in order to be valid.
	//
	// This MUST be treated as read only in the lifetime of the exec.
	io              cmd.UpstreamIO
	processDone     chan struct{}
	processDoneOnce sync.Once

	// sl is the state lock that MUST be held to safely read/write any of the
	// following members.
	sl         sync.Mutex
	state      shimExecState
	pid        int
	exitStatus uint32
	exitedAt   time.Time
	p          *cmd.Cmd

	// exited is a wait block which waits async for the process to exit.
	exited     chan struct{}
	exitedOnce sync.Once
}

func (ke *kryptonExec) ID() string {
	return ke.id
}

func (ke *kryptonExec) Pid() int {
	ke.sl.Lock()
	defer ke.sl.Unlock()
	return ke.pid
}

func (ke *kryptonExec) State() shimExecState {
	ke.sl.Lock()
	defer ke.sl.Unlock()
	return ke.state
}

func (ke *kryptonExec) Status() *task.StateResponse {
	ke.sl.Lock()
	defer ke.sl.Unlock()

	var s containerd_v1_types.Status
	switch ke.state {
	case shimExecStateCreated:
		s = containerd_v1_types.StatusCreated
	case shimExecStateRunning:
		s = containerd_v1_types.StatusRunning
	case shimExecStateExited:
		s = containerd_v1_types.StatusStopped
	}

	return &task.StateResponse{
		ID:         ke.tid,
		ExecID:     ke.id,
		Bundle:     ke.bundle,
		Pid:        uint32(ke.pid),
		Status:     s,
		Stdin:      ke.io.StdinPath(),
		Stdout:     ke.io.StdoutPath(),
		Stderr:     ke.io.StderrPath(),
		Terminal:   ke.io.Terminal(),
		ExitStatus: ke.exitStatus,
		ExitedAt:   ke.exitedAt,
	}
}

func (ke *kryptonExec) startInternal(ctx context.Context, initializeContainer bool) (err error) {
	ke.sl.Lock()
	defer ke.sl.Unlock()
	if ke.state != shimExecStateCreated {
		return newExecInvalidStateError(ke.tid, ke.id, ke.state, "start")
	}
	defer func() {
		if err != nil {
			ke.exitFromCreatedL(ctx, 1)
		}
	}()
	// The container may need to be started
	if initializeContainer {
		log.G(ctx).Debug("PBOZZAY: Initializing Krypton container for Exec!")
		err = ke.c.Start(ctx)
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				ke.c.Terminate(ctx)
				ke.c.Close()
			}
		}()
	}

	/*
		RESUME HERE
		if len(req.Args) == 0 {
			return 0, errors.New("missing command")
		}
		np, err := NewNpipeIO(ctx, req.Stdin, req.Stdout, req.Stderr, req.Terminal)
		if err != nil {
			return 0, err
		}
		defer np.Close(ctx)

		cmd := CommandContext(ctx, vm, req.Args[0], req.Args[1:]...)
		if req.Workdir != "" {
			cmd.Spec.Cwd = req.Workdir
		}
		if vm.OS() == "windows" {
			cmd.Spec.User.Username = `NT AUTHORITY\SYSTEM`
		}

		cmd.Spec.Terminal = req.Terminal
		cmd.Stdin = np.Stdin()
		cmd.Stdout = np.Stdout()
		cmd.Stderr = np.Stderr()
		cmd.Log = log.G(ctx).WithField(logfields.UVMID, vm.ID())
		err = cmd.Run()

		return cmd.ExitState.ExitCode(), err
	*/

	// TODO(pbozzay): Create the CMD created correctly.
	//np, err := cmd.NewNpipeIO(ctx, ke.io.StdinPath(), ke.io.StdoutPath(), ke.io.StderrPath(), ke.io.Terminal())

	cmd := &cmd.Cmd{
		Host:   ke.c,
		Stdin:  ke.io.Stdin(),
		Stdout: ke.io.Stdout(),
		Stderr: ke.io.Stderr(),
		Log: log.G(ctx).WithFields(logrus.Fields{
			"tid": ke.tid,
			"eid": ke.id,
		}),
		CopyAfterExitTimeout: time.Second * 1,
	}
	if ke.isWCOW || ke.id != ke.tid {
		// An init exec passes the process as part of tke config. We only pass
		// the spec if this is a true exec.
		cmd.Spec = ke.spec
	}
	err = cmd.Start()
	if err != nil {
		return err
	}
	ke.p = cmd

	// Assign tke PID and transition the state.
	ke.pid = ke.p.Process.Pid()
	ke.state = shimExecStateRunning

	// Publish tke task/exec start event. This MUST happen before waitForExit to
	// avoid publishing the exit previous to the start.
	if ke.id != ke.tid {
		ke.events.publishEvent(
			ctx,
			runtime.TaskExecStartedEventTopic,
			&eventstypes.TaskExecStarted{
				ContainerID: ke.tid,
				ExecID:      ke.id,
				Pid:         uint32(ke.pid),
			})
	} else {
		ke.events.publishEvent(
			ctx,
			runtime.TaskStartEventTopic,
			&eventstypes.TaskStart{
				ContainerID: ke.tid,
				Pid:         uint32(ke.pid),
			})
	}

	// wait in the background for the exit.
	go ke.waitForExit()
	return nil
}

func (ke *kryptonExec) Start(ctx context.Context) (err error) {
	// If ke.id == ke.tid then this is the init exec.
	// We need to initialize the container itself before starting this exec.
	return ke.startInternal(ctx, ke.id == ke.tid)
}

func (ke *kryptonExec) Kill(ctx context.Context, signal uint32) error {
	ke.sl.Lock()
	defer ke.sl.Unlock()
	switch ke.state {
	case shimExecStateCreated:
		ke.exitFromCreatedL(ctx, 1)
		return nil
	case shimExecStateRunning:
		supported := false
		if osversion.Get().Build >= osversion.RS5 {
			//supported = ke.host == nil || ke.host.SignalProcessSupported()
			// TODO(pbozzay): Change this
			supported = false
		}
		var options interface{}
		var err error
		if ke.isWCOW {
			var opt *guestrequest.SignalProcessOptionsWCOW
			opt, err = signals.ValidateWCOW(int(signal), supported)
			if opt != nil {
				options = opt
			}
		} else {
			var opt *guestrequest.SignalProcessOptionsLCOW
			opt, err = signals.ValidateLCOW(int(signal), supported)
			if opt != nil {
				options = opt
			}
		}
		if err != nil {
			return errors.Wrapf(errdefs.ErrFailedPrecondition, "signal %d: %v", signal, err)
		}
		var delivered bool
		if supported && options != nil {
			delivered, err = ke.p.Process.Signal(ctx, options)
		} else {
			// legacy path before signals support OR if WCOW with signals
			// support needs to issue a terminate.
			delivered, err = ke.p.Process.Kill(ctx)
		}
		if err != nil {
			return err
		}
		if !delivered {
			return errors.Wrapf(errdefs.ErrNotFound, "exec: '%s' in task: '%s' not found", ke.id, ke.tid)
		}
		return nil
	case shimExecStateExited:
		return errors.Wrapf(errdefs.ErrNotFound, "exec: '%s' in task: '%s' not found", ke.id, ke.tid)
	default:
		return newExecInvalidStateError(ke.tid, ke.id, ke.state, "kill")
	}
}

func (ke *kryptonExec) ResizePty(ctx context.Context, width, height uint32) error {
	ke.sl.Lock()
	defer ke.sl.Unlock()
	if !ke.io.Terminal() {
		return errors.Wrapf(errdefs.ErrFailedPrecondition, "exec: '%s' in task: '%s' is not a tty", ke.id, ke.tid)
	}

	if ke.state == shimExecStateRunning {
		return ke.p.Process.ResizeConsole(ctx, uint16(width), uint16(height))
	}
	return nil
}

func (ke *kryptonExec) CloseIO(ctx context.Context, stdin bool) error {
	// If we have any upstream IO we close the upstream connection. This will
	// unblock the `io.Copy` in the `Start()` call which will signal
	// `he.p.CloseStdin()`. If `he.io.Stdin()` is already closed this is safe to
	// call multiple times.
	ke.io.CloseStdin(ctx)
	return nil
}

func (ke *kryptonExec) Wait() *task.StateResponse {
	<-ke.exited
	return ke.Status()
}

func (ke *kryptonExec) ForceExit(ctx context.Context, status int) {
	ke.sl.Lock()
	defer ke.sl.Unlock()
	if ke.state != shimExecStateExited {
		switch ke.state {
		case shimExecStateCreated:
			ke.exitFromCreatedL(ctx, status)
		case shimExecStateRunning:
			// Kill the process to unblock `ke.waitForExit`
			ke.p.Process.Kill(ctx)
		}
	}
}

// exitFromCreatedL transitions the shim to the exited state from the created
// state. It is the callers responsibility to hold `he.sl` for the durration of
// this transition.
//
// This call is idempotent and will not affect any state if the shim is already
// in the `shimExecStateExited` state.
//
// To transition for a created state the following must be done:
//
// 1. Issue `he.processDoneCancel` to unblock the goroutine
// `he.waitForContainerExit()``.
//
// 2. Set `he.state`, `he.exitStatus` and `he.exitedAt` to the exited values.
//
// 3. Release any upstream IO resources that were never used in a copy.
//
// 4. Close `he.exited` channel to unblock any waiters who might have called
// `Create`/`Wait`/`Start` which is a valid pattern.
//
// We DO NOT send the async `TaskExit` event because we never would have sent
// the `TaskStart`/`TaskExecStarted` event.
func (he *kryptonExec) exitFromCreatedL(ctx context.Context, status int) {
	if he.state != shimExecStateExited {
		// Avoid logging the force if we already exited gracefully
		log.G(ctx).WithField("status", status).Debug("kryptonExec::exitFromCreatedL")

		// Unblock the container exit goroutine
		he.processDoneOnce.Do(func() { close(he.processDone) })
		// Transition this exec
		he.state = shimExecStateExited
		he.exitStatus = uint32(status)
		he.exitedAt = time.Now()
		// Release all upstream IO connections (if any)
		he.io.Close(ctx)
		// Free any waiters
		he.exitedOnce.Do(func() {
			close(he.exited)
		})
	}
}

// waitForExit waits for the `he.p` to exit. This MUST only be called after a
// successful call to `Create` and MUST not be called more than once.
//
// This MUST be called via a goroutine.
//
// In the case of an exit from a running process the following must be done:
//
// 1. Wait for `he.p` to exit.
//
// 2. Issue `he.processDoneCancel` to unblock the goroutine
// `he.waitForContainerExit()` (if still running). We do this early to avoid the
// container exit also attempting to kill the process. However this race
// condition is safe and handled.
//
// 3. Capture the process exit code and set `he.state`, `he.exitStatus` and
// `he.exitedAt` to the exited values.
//
// 4. Wait for all IO to complete and release any upstream IO connections.
//
// 5. Send the async `TaskExit` to upstream listeners of any events.
//
// 6. Close `he.exited` channel to unblock any waiters who might have called
// `Create`/`Wait`/`Start` which is a valid pattern.
//
// 7. Finally, save the UVM and this container as a template if specified.
func (he *kryptonExec) waitForExit() {
	ctx, span := trace.StartSpan(context.Background(), "kryptonExec::waitForExit")
	defer span.End()
	span.AddAttributes(
		trace.StringAttribute("tid", he.tid),
		trace.StringAttribute("eid", he.id))

	err := he.p.Process.Wait()
	if err != nil {
		log.G(ctx).WithError(err).Error("failed process Wait")
	}

	// Issue the process cancellation to unblock the container wait as early as
	// possible.
	he.processDoneOnce.Do(func() { close(he.processDone) })

	code, err := he.p.Process.ExitCode()
	if err != nil {
		log.G(ctx).WithError(err).Error("failed to get ExitCode")
	} else {
		log.G(ctx).WithField("exitCode", code).Debug("exited")
	}

	he.sl.Lock()
	he.state = shimExecStateExited
	he.exitStatus = uint32(code)
	he.exitedAt = time.Now()
	he.sl.Unlock()

	// Wait for all IO copies to complete and free the resources.
	he.p.Wait()
	he.io.Close(ctx)

	// Only send the `runtime.TaskExitEventTopic` notification if this is a true
	// exec. For the `init` exec this is handled in task teardown.
	if he.tid != he.id {
		// We had a valid process so send the exited notification.
		he.events.publishEvent(
			ctx,
			runtime.TaskExitEventTopic,
			&eventstypes.TaskExit{
				ContainerID: he.tid,
				ID:          he.id,
				Pid:         uint32(he.pid),
				ExitStatus:  he.exitStatus,
				ExitedAt:    he.exitedAt,
			})
	}

	// Free any waiters.
	he.exitedOnce.Do(func() {
		close(he.exited)
	})
}

// waitForContainerExit waits for `he.c` to exit. Depending on the exec's state
// will forcibly transition this exec to the exited state and unblock any
// waiters.
//
// This MUST be called via a goroutine at exec create.
func (he *kryptonExec) waitForContainerExit() {
	ctx, span := trace.StartSpan(context.Background(), "kryptonExec::waitForContainerExit")
	defer span.End()
	span.AddAttributes(
		trace.StringAttribute("tid", he.tid),
		trace.StringAttribute("eid", he.id))

	cexit := make(chan struct{})
	go func() {
		he.c.Wait()
		close(cexit)
	}()
	select {
	case <-cexit:
		// Container exited first. We need to force the process into the exited
		// state and cleanup any resources
		he.sl.Lock()
		switch he.state {
		case shimExecStateCreated:
			he.exitFromCreatedL(ctx, 1)
		case shimExecStateRunning:
			// Kill the process to unblock `he.waitForExit`.
			he.p.Process.Kill(ctx)
		}
		he.sl.Unlock()
	case <-he.processDone:
		// Process exited first. This is the normal case do nothing because
		// `he.waitForExit` will release any waiters.
	}
}
