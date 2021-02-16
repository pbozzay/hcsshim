package uvm

import (
	"context"

	"github.com/Microsoft/hcsshim/internal/cow"
	"github.com/Microsoft/hcsshim/internal/schema1"
	hcsschema "github.com/Microsoft/hcsshim/internal/schema2"
	"github.com/pkg/errors"
)

// KryptonContainer implements the cow.Container interface.
type KryptonContainer struct {
	*UtilityVM
}

// Compile-time check to make sure KryptonContainer implements
// the cow.Container interface.
var _ cow.Container = &KryptonContainer{}

// Shutdown asynchronously shuts down the Krypton.
func (container *KryptonContainer) Shutdown(ctx context.Context) error {
	return container.hcsSystem.Shutdown(ctx)
}

// Modify sends a modify request to the container.
func (container *KryptonContainer) Modify(ctx context.Context, config interface{}) (err error) {
	return errors.New("Modify API is not implemented for Krypton")
}

// Properties returns the requested container properties targeting a V1 schema container.
func (container *KryptonContainer) Properties(ctx context.Context, types ...schema1.PropertyType) (_ *schema1.ContainerProperties, err error) {
	return nil, errors.New("Properties API is not implemented for Krypton")
}

// PropertiesV2 returns the requested container properties targeting a V2 schema container.
func (container *KryptonContainer) PropertiesV2(ctx context.Context, types ...hcsschema.PropertyType) (_ *hcsschema.Properties, err error) {
	return nil, errors.New("PropertiesV2 API is not implemented for Krypton")
}

/*
// Start synchronously starts the container.
func (container *KryptonContainer) Start() error {
	return convertSystemError(container.system.Start(context.Background()), container)
}

// OS returns the operating system of the container, "linux" or "windows".
func (c *KryptonContainer) OS() string {
	return c.gc.os
}

// IsOCI specifies whether CreateProcess should be called with an OCI
// specification in its input.
func (c *KryptonContainer) IsOCI() bool {
	return c.gc.os != "windows"
}

// Shutdown sends a graceful shutdown request to the container. The container
// might not be terminated by the time the request completes (and might never
// terminate).
func (c *KryptonContainer) Shutdown(ctx context.Context) (err error) {
}

// Terminate sends a forceful terminate request to the container. The container
// might not be terminated by the time the request completes (and might never
// terminate).
//func (c *KryptonContainer) Terminate(ctx context.Context) (err error) {
	// Already implemented by UtilityVM
//}

// Wait waits for the container to terminate (or Close to be called, or the
// guest connection to terminate).
func (c *Container) Wait() error {
	select {
	case <-c.notifyCh:
		return nil
	case <-c.closeCh:
		return errors.New("container closed")
	}
}

func (c *Container) waitBackground() {
	ctx, span := trace.StartSpan(context.Background(), "gcs::Container::waitBackground")
	defer span.End()
	span.AddAttributes(trace.StringAttribute("cid", c.id))

	err := c.Wait()
	log.G(ctx).Debug("container exited")
	oc.SetSpanStatus(span, err)
}
*/
