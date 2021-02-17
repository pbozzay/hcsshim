package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	eventstypes "github.com/containerd/containerd/api/events"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/runtime"
	"github.com/containerd/containerd/runtime/v2/task"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.opencensus.io/trace"

	"github.com/Microsoft/hcsshim/internal/oci"
	"github.com/Microsoft/hcsshim/internal/uvm"

	"github.com/Microsoft/hcsshim/cmd/containerd-shim-runhcs-v1/options"
	"github.com/Microsoft/hcsshim/cmd/containerd-shim-runhcs-v1/stats"
	"github.com/Microsoft/hcsshim/internal/cmd"
	"github.com/Microsoft/hcsshim/internal/cow"
	"github.com/Microsoft/hcsshim/internal/hcs"
	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/schema1"
	hcsschema "github.com/Microsoft/hcsshim/internal/schema2"
	"github.com/Microsoft/hcsshim/internal/shimdiag"
)

// newKryptonTask creates a Krypton container and its init exec process in
// the `shimExecCreated` state and returns the task that tracks its lifetime.
func newKryptonTask(
	ctx context.Context,
	events publisher,
	req *task.CreateTaskRequest,
	s *specs.Spec) (_ shimTask, err error) {

	owner := filepath.Base(os.Args[0])

	log.G(ctx).WithFields(logrus.Fields{
		"tid":   req.ID,
		"owner": owner,
		"spec":  s,
	}).Debug("newKryptonTask")

	// Rigkt now this will look at the metadata rather than the OCI spec to get container
	// parameters for the UVM.
	//
	// TODO(pbozzay): Create a version of this function that can take a spec and convert the
	// 				  container portion of the request to a UVM spec.
	opts, err := oci.SpecToKryptonUVMCreateOpts(ctx, s, fmt.Sprintf("%s@vm", req.ID), owner)
	if err != nil {
		return nil, err
	}

	var wcow *uvm.UtilityVM
	var krypton *uvm.KryptonContainer

	switch opts.(type) {
	case *uvm.OptionsLCOW:
		return nil, errors.Wrap(errdefs.ErrFailedPrecondition, "Krypton tasks are not supported for LCOW.")
	case *uvm.OptionsWCOW:
		wopts := (opts).(*uvm.OptionsWCOW)

		// In order for the UVM sandbox.vhdx not to collide with the actual
		// nested Argon sandbox.vhdx we append the \vm folder to the last
		// entry in the list.
		log.G(ctx).WithFields(logrus.Fields{
			"LAYERS": s.Windows.LayerFolders,
			"spec":   s,
		}).Debug("PBOZZA: Layers were...")

		layersLen := len(s.Windows.LayerFolders)
		layers := make([]string, layersLen)
		copy(layers, s.Windows.LayerFolders)

		vmPath := filepath.Join(layers[layersLen-1], "vm")
		err := os.MkdirAll(vmPath, 0)
		if err != nil {
			return nil, err
		}
		layers[layersLen-1] = vmPath
		wopts.LayerFolders = layers

		log.G(ctx).WithFields(logrus.Fields{
			"tid":    req.ID,
			"vmPath": vmPath,
			"spec":   s,
		}).Debug("newKryptonTask:SetPath to new location(currently expecting \\vm")

		io, err := cmd.NewNpipeIO(ctx, req.Stdin, req.Stdout, req.Stderr, req.Terminal)
		if err != nil {
			return nil, err
		}

		// This is pulled from the Argon newHcsTask function. It needs to be ported to Krypton.
		/*
			var netNS string
			if s.Windows != nil &&
				s.Windows.Network != nil {
				netNS = s.Windows.Network.NetworkNamespace
			}
		*/

		// Create a krypton cow.Container but do not start it.
		wcow, err = uvm.CreateWCOW(ctx, wopts)
		if err != nil {
			return nil, err
		}
		krypton = &uvm.KryptonContainer{UtilityVM: wcow}

		kt := &kryptonTask{
			events:   events,
			id:       req.ID,
			c:        krypton,
			closed:   make(chan struct{}),
			taskSpec: s,
		}

		// Create an exec inside of this container; this will start the container.
		kt.init = newKryptonExec(
			ctx,
			events,
			req.ID,
			krypton,
			req.ID,
			req.Bundle,
			s.Process,
			io,
		)

		// In the normal case the `Signal` call from the caller killed this task's
		// init process. Or the init process ran to completion - this will mostly
		// happen when we are creating a template and want to wait for init process
		// to finish before we save the template. In such cases do not tear down the
		// container after init exits - because we need the container in the template
		go kt.waitInitExit(true)

		// Publish the created event
		kt.events.publishEvent(
			ctx,
			runtime.TaskCreateEventTopic,
			&eventstypes.TaskCreate{
				ContainerID: req.ID,
				Bundle:      req.Bundle,
				Rootfs:      req.Rootfs,
				IO: &eventstypes.TaskIO{
					Stdin:    req.Stdin,
					Stdout:   req.Stdout,
					Stderr:   req.Stderr,
					Terminal: req.Terminal,
				},
				Checkpoint: "",
				Pid:        uint32(kt.init.Pid()),
			})
		return kt, nil
	}

	return nil, errors.Wrap(errdefs.ErrFailedPrecondition, "Invalid conditions for newKryptonTask")
}

var _ = (shimTask)(&kryptonTask{})

// kryptonTask is a generic task that represents a Krypton hypervisor
// isolated Container. When the init task/exec is stopped the container
// itself will be stopped as well.
type kryptonTask struct {
	events publisher
	// id is the id of this task when it is created.
	//
	// It MUST be treated as read only in the liftetime of the task.
	id string
	// c is the container backing this task.
	//
	// It MUST be treated as read only in the lifetime of this task EXCEPT after
	// a Kill to the init task in which it must be shutdown.
	c cow.Container
	// cr is the container resources this task is holding.
	//
	// It MUST be treated as read only in the lifetime of this task EXCEPT after
	// a Kill to the init task in which all resources must be released.
	//cr *resources.Resources
	// init is the init process of the container.
	//
	// Note: the invariant `container state == init.State()` MUST be true. IE:
	// if the init process exits the container as a whole and all exec's MUST
	// exit.
	//
	// It MUST be treated as read only in the lifetime of the task.
	init shimExec
	// ecl is the exec create lock for all non-init execs and MUST be held
	// durring create to prevent ID duplication.
	ecl   sync.Mutex
	execs sync.Map

	closed    chan struct{}
	closeOnce sync.Once
	// closeHostOnce is used to close `host`. This will only be used if
	// `ownsHost==true` and `host != nil`.
	closeHostOnce sync.Once

	// templateID represents the id of the template container from which this container
	// is cloned. The parent UVM (inside which this container is running) identifies this
	// container with it's original id (i.e the id that was assigned to this container
	// at the time of template creation i.e the templateID). Hence, every request that
	// is sent to the GCS must actually use templateID to reference this container.
	// A non-empty templateID specifies that this task was cloned.
	templateID string

	// taskSpec represents the spec/configuration for this task.
	taskSpec *specs.Spec
}

func (kt *kryptonTask) ID() string {
	return kt.id
}

// TODO(pbozzay): Implement this.
func (kt *kryptonTask) CreateExec(ctx context.Context, req *task.ExecProcessRequest, spec *specs.Process) error {
	kt.ecl.Lock()
	defer kt.ecl.Unlock()

	// If the task exists or we got a request for "" which is the init task
	// fail.
	if _, loaded := kt.execs.Load(req.ExecID); loaded || req.ExecID == "" {
		return errors.Wrapf(errdefs.ErrAlreadyExists, "exec: '%s' in task: '%s' already exists", req.ExecID, kt.id)
	}

	if kt.init.State() != shimExecStateRunning {
		return errors.Wrapf(errdefs.ErrFailedPrecondition, "exec: '' in task: '%s' must be running to create additional execs", kt.id)
	}

	io, err := cmd.NewUpstreamIO(ctx, req.ID, req.Stdout, req.Stderr, req.Stdin, req.Terminal)
	if err != nil {
		return err
	}

	he := newHcsExec(
		ctx,
		kt.events,
		kt.id,
		nil, // pbozza: was kt.host
		kt.c,
		req.ExecID,
		kt.init.Status().Bundle,
		true,
		spec,
		io,
	)

	kt.execs.Store(req.ExecID, he)

	// Publish the created event
	kt.events.publishEvent(
		ctx,
		runtime.TaskExecAddedEventTopic,
		&eventstypes.TaskExecAdded{
			ContainerID: kt.id,
			ExecID:      req.ExecID,
		})

	return nil
}

func (kt *kryptonTask) GetExec(eid string) (shimExec, error) {
	if eid == "" {
		return kt.init, nil
	}
	raw, loaded := kt.execs.Load(eid)
	if !loaded {
		return nil, errors.Wrapf(errdefs.ErrNotFound, "exec: '%s' in task: '%s' not found", eid, kt.id)
	}
	return raw.(shimExec), nil
}

func (kt *kryptonTask) KillExec(ctx context.Context, eid string, signal uint32, all bool) error {
	e, err := kt.GetExec(eid)
	if err != nil {
		return err
	}
	if all && eid != "" {
		return errors.Wrapf(errdefs.ErrFailedPrecondition, "cannot signal all for non-empty exec: '%s'", eid)
	}
	if all {
		// We are in a kill all on the init task. Signal everything.
		kt.execs.Range(func(key, value interface{}) bool {
			err := value.(shimExec).Kill(ctx, signal)
			if err != nil {
				log.G(ctx).WithFields(logrus.Fields{
					"eid":           key,
					logrus.ErrorKey: err,
				}).Warn("failed to kill exec in task")
			}

			// iterate all
			return false
		})
	}

	return e.Kill(ctx, signal)
}

func (kt *kryptonTask) DeleteExec(ctx context.Context, eid string) (int, uint32, time.Time, error) {
	e, err := kt.GetExec(eid)
	if err != nil {
		return 0, 0, time.Time{}, err
	}
	if eid == "" {
		// We are deleting the init exec. Forcibly exit any additional exec's.
		kt.execs.Range(func(key, value interface{}) bool {
			ex := value.(shimExec)
			if s := ex.State(); s != shimExecStateExited {
				ex.ForceExit(ctx, 1)
			}

			// iterate next
			return false
		})
	}
	switch state := e.State(); state {
	case shimExecStateCreated:
		e.ForceExit(ctx, 0)
	case shimExecStateRunning:
		return 0, 0, time.Time{}, newExecInvalidStateError(kt.id, eid, state, "delete")
	}
	status := e.Status()
	if eid != "" {
		kt.execs.Delete(eid)
	}

	// Publish the deleted event
	kt.events.publishEvent(
		ctx,
		runtime.TaskDeleteEventTopic,
		&eventstypes.TaskDelete{
			ContainerID: kt.id,
			ID:          eid,
			Pid:         status.Pid,
			ExitStatus:  status.ExitStatus,
			ExitedAt:    status.ExitedAt,
		})

	return int(status.Pid), status.ExitStatus, status.ExitedAt, nil
}

func (kt *kryptonTask) Pids(ctx context.Context) ([]options.ProcessDetails, error) {
	// Map all user created exec's to pid/exec-id
	pidMap := make(map[int]string)
	kt.execs.Range(func(key, value interface{}) bool {
		ex := value.(shimExec)
		pidMap[ex.Pid()] = ex.ID()

		// Iterate all
		return false
	})
	pidMap[kt.init.Pid()] = kt.init.ID()

	// Get the guest pids
	props, err := kt.c.Properties(ctx, schema1.PropertyTypeProcessList)
	if err != nil {
		return nil, err
	}

	// Copy to pid/exec-id pair's
	pairs := make([]options.ProcessDetails, len(props.ProcessList))
	for i, p := range props.ProcessList {
		pairs[i].ImageName = p.ImageName
		pairs[i].CreatedAt = p.CreateTimestamp
		pairs[i].KernelTime_100Ns = p.KernelTime100ns
		pairs[i].MemoryCommitBytes = p.MemoryCommitBytes
		pairs[i].MemoryWorkingSetPrivateBytes = p.MemoryWorkingSetPrivateBytes
		pairs[i].MemoryWorkingSetSharedBytes = p.MemoryWorkingSetSharedBytes
		pairs[i].ProcessID = p.ProcessId
		pairs[i].UserTime_100Ns = p.KernelTime100ns

		if eid, ok := pidMap[int(p.ProcessId)]; ok {
			pairs[i].ExecID = eid
		}
	}
	return pairs, nil
}

func (kt *kryptonTask) Wait() *task.StateResponse {
	<-kt.closed
	return kt.init.Wait()
}

func (kt *kryptonTask) waitInitExit(destroyContainer bool) {
	ctx, span := trace.StartSpan(context.Background(), "kryptonTask::waitInitExit")
	defer span.End()
	span.AddAttributes(trace.StringAttribute("tid", kt.id))

	// Wait for it to exit on its own
	kt.init.Wait()

	if destroyContainer {
		// Close the host and event the exit
		kt.close(ctx)
	} else {
		// Close the container's host, but do not close or terminate the container itself
		kt.closeHost(ctx)
	}

}

// close shuts down the container that is owned by this task and if
// `kt.ownsHost` will shutdown the hosting VM the container was placed in.
//
// NOTE: For Windows process isolated containers `kt.ownsHost==true && kt.host
// == nil`.
func (kt *kryptonTask) close(ctx context.Context) {
	kt.closeOnce.Do(func() {
		log.G(ctx).Debug("kryptonTask::closeOnce")

		// kt.c should never be nil for a real task but in testing we stub
		// this to avoid a nil dereference. We really should introduce a
		// method or interface for kt.c operations that we can stub for
		// testing.
		if kt.c != nil {
			// Do our best attempt to tear down the container.
			var werr error
			ch := make(chan struct{})
			go func() {
				werr = kt.c.Wait()
				close(ch)
			}()
			err := kt.c.Shutdown(ctx)
			if err != nil {
				log.G(ctx).WithError(err).Error("failed to shutdown container")
			} else {
				t := time.NewTimer(time.Second * 30)
				select {
				case <-ch:
					err = werr
					t.Stop()
					if err != nil {
						log.G(ctx).WithError(err).Error("failed to wait for container shutdown")
					}
				case <-t.C:
					log.G(ctx).WithError(hcs.ErrTimeout).Error("failed to wait for container shutdown")
				}
			}

			if err != nil {
				err = kt.c.Terminate(ctx)
				if err != nil {
					log.G(ctx).WithError(err).Error("failed to terminate container")
				} else {
					t := time.NewTimer(time.Second * 30)
					select {
					case <-ch:
						err = werr
						t.Stop()
						if err != nil {
							log.G(ctx).WithError(err).Error("failed to wait for container terminate")
						}
					case <-t.C:
						log.G(ctx).WithError(hcs.ErrTimeout).Error("failed to wait for container terminate")
					}
				}
			}

			// Release any resources associated with the container.
			// TODO(pbozzay): Not sure what to do here.
			/*
				if err := resources.ReleaseResources(ctx, kt.cr, kt.host, true); err != nil {
					log.G(ctx).WithError(err).Error("failed to release container resources")
				}
			*/

			// Close the container handle invalidating all future access.
			if err := kt.c.Close(); err != nil {
				log.G(ctx).WithError(err).Error("failed to close container")
			}
		}
		kt.closeHost(ctx)
	})
}

// closeHost safely closes the hosting UVM if this task is the owner. Once
// closed and all resources released it events the `runtime.TaskExitEventTopic`
// for all upstream listeners.
//
// Note: If this is a process isolated task the hosting UVM is simply a `noop`.
//
// This call is idempotent and safe to call multiple times.
func (kt *kryptonTask) closeHost(ctx context.Context) {
	kt.closeHostOnce.Do(func() {
		log.G(ctx).Debug("kryptonTask::closeHostOnce")

		// Send the `init` exec exit notification always.
		exit := kt.init.Status()
		kt.events.publishEvent(
			ctx,
			runtime.TaskExitEventTopic,
			&eventstypes.TaskExit{
				ContainerID: kt.id,
				ID:          exit.ID,
				Pid:         uint32(exit.Pid),
				ExitStatus:  exit.ExitStatus,
				ExitedAt:    exit.ExitedAt,
			})
		close(kt.closed)
	})
}

func (kt *kryptonTask) ExecInHost(ctx context.Context, req *shimdiag.ExecProcessRequest) (int, error) {
	/*
		if kt.host == nil {
			return cmd.ExecInShimHost(ctx, req)
		}
		return cmd.ExecInUvm(ctx, kt.host, req)
	*/
	return -1, fmt.Errorf("ExecInHost is not implemented for Krypton containers")
}

func (kt *kryptonTask) DumpGuestStacks(ctx context.Context) string {
	return ("DumpGuestStacks is not implemented for Krypton containers.")
}

func (kt *kryptonTask) Share(ctx context.Context, req *shimdiag.ShareRequest) error {
	// TODO(pbozzay): Unsure what this should do when called.
	return errors.New("Share API is not implemented for Krypton containers.")

	/*
		if kt.host == nil {
			return errTaskNotIsolated
		}
		// For hyper-v isolated WCOW the task used isn't the standard kryptonTask so we
		// only have to deal with the LCOW case here.
		st, err := os.Stat(req.HostPath)
		if err != nil {
			return fmt.Errorf("could not open '%s' path on host: %s", req.HostPath, err)
		}
		var (
			hostPath       string = req.HostPath
			restrictAccess bool
			fileName       string
			allowedNames   []string
		)
		if !st.IsDir() {
			hostPath, fileName = filepath.Split(hostPath)
			allowedNames = append(allowedNames, fileName)
			restrictAccess = true
		}
		_, err = kt.host.AddPlan9(ctx, hostPath, req.UvmPath, req.ReadOnly, restrictAccess, allowedNames)
		return err
	*/
}

func kryptonPropertiesToWindowsStats(props *hcsschema.Properties) *stats.Statistics_Windows {
	wcs := &stats.Statistics_Windows{Windows: &stats.WindowsContainerStatistics{}}
	if props.Statistics != nil {
		wcs.Windows.Timestamp = props.Statistics.Timestamp
		wcs.Windows.ContainerStartTime = props.Statistics.ContainerStartTime
		wcs.Windows.UptimeNS = props.Statistics.Uptime100ns * 100
		if props.Statistics.Processor != nil {
			wcs.Windows.Processor = &stats.WindowsContainerProcessorStatistics{
				TotalRuntimeNS:  props.Statistics.Processor.TotalRuntime100ns * 100,
				RuntimeUserNS:   props.Statistics.Processor.RuntimeUser100ns * 100,
				RuntimeKernelNS: props.Statistics.Processor.RuntimeKernel100ns * 100,
			}
		}
		if props.Statistics.Memory != nil {
			wcs.Windows.Memory = &stats.WindowsContainerMemoryStatistics{
				MemoryUsageCommitBytes:            props.Statistics.Memory.MemoryUsageCommitBytes,
				MemoryUsageCommitPeakBytes:        props.Statistics.Memory.MemoryUsageCommitPeakBytes,
				MemoryUsagePrivateWorkingSetBytes: props.Statistics.Memory.MemoryUsagePrivateWorkingSetBytes,
			}
		}
		if props.Statistics.Storage != nil {
			wcs.Windows.Storage = &stats.WindowsContainerStorageStatistics{
				ReadCountNormalized:  props.Statistics.Storage.ReadCountNormalized,
				ReadSizeBytes:        props.Statistics.Storage.ReadSizeBytes,
				WriteCountNormalized: props.Statistics.Storage.WriteCountNormalized,
				WriteSizeBytes:       props.Statistics.Storage.WriteSizeBytes,
			}
		}
	}
	return wcs
}

func (kt *kryptonTask) Stats(ctx context.Context) (*stats.Statistics, error) {
	s := &stats.Statistics{}
	props, err := kt.c.PropertiesV2(ctx, hcsschema.PTStatistics)
	if err != nil && !isStatsNotFound(err) {
		return nil, err
	}
	if props != nil {
		s.Container = kryptonPropertiesToWindowsStats(props)
	}
	return s, nil
}
