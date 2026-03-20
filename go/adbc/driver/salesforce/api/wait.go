package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
	"github.com/cenkalti/backoff/v5"
)

// newBackoff creates an ExponentialBackOff from the given config.
// If cfg is nil, DefaultBackoffConfig() is used.
func (c *Client) newBackoff(cfg *types.BackoffConfig) *backoff.ExponentialBackOff {
	if cfg == nil {
		d := types.DefaultBackoffConfig()
		cfg = &d
	}
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = cfg.InitialInterval
	b.MaxInterval = cfg.MaxInterval
	b.Multiplier = cfg.Multiplier
	b.RandomizationFactor = cfg.RandomFactor
	return b
}

// WaitForTransformStatus polls a data transform until its status matches any of the targets.
// If the transform enters Deleting status (and Deleting is not a target), a permanent error is returned.
func (c *Client) WaitForTransformStatus(ctx context.Context, name string, cfg *types.BackoffConfig, targets ...types.Status) (*types.DataTransform, error) {
	return backoff.Retry(
		ctx,
		func() (*types.DataTransform, error) {
			dt, err := c.GetDataTransform(ctx, name)
			if err != nil {
				return nil, fmt.Errorf("waiting for transform status: %w", err)
			}

			if dt.Status.IsOneOf(targets...) {
				return dt, nil
			}

			if dt.Status.IsDeleting() {
				return dt, backoff.Permanent(fmt.Errorf("data transform is being deleted"))
			}

			return dt, fmt.Errorf("data transform status is %q, waiting for %v", dt.Status, targets)
		},
		backoff.WithBackOff(c.newBackoff(cfg)),
		backoff.WithNotify(func(err error, dur time.Duration) {
			c.GetLogger().DebugContext(ctx, "WaitForTransformStatus", slog.Any("err", err), slog.Duration("retry-in", dur))
		}),
	)
}

// WaitForTransformDeleted polls until a data transform returns 404.
func (c *Client) WaitForTransformDeleted(ctx context.Context, name string, cfg *types.BackoffConfig) error {
	_, err := backoff.Retry(
		ctx,
		func() (_ struct{}, _ error) {
			_, err := c.GetDataTransform(ctx, name)
			if err == nil {
				return struct{}{}, fmt.Errorf("transform %s still exists", name)
			}
			var sfErr *SalesforceError
			if errors.As(err, &sfErr) && sfErr.IsNotFound() {
				return struct{}{}, nil
			}
			return struct{}{}, fmt.Errorf("waiting for transform deleted: %w", err)
		},
		backoff.WithBackOff(c.newBackoff(cfg)),
		backoff.WithNotify(func(err error, dur time.Duration) {
			c.GetLogger().DebugContext(ctx, "WaitForTransformDeleted", slog.Any("err", err), slog.Duration("retry-in", dur))
		}),
	)
	return err
}

// WaitForTransformRun polls the run status of a data transform until it reaches a terminal state.
func (c *Client) WaitForTransformRun(ctx context.Context, name string, cfg *types.BackoffConfig) (*types.DataTransform, error) {
	return backoff.Retry(
		ctx,
		func() (*types.DataTransform, error) {
			if _, err := c.RefreshDataTransformStatus(ctx, name); err != nil {
				return nil, fmt.Errorf("refreshing transform status: %w", err)
			}

			dt, err := c.GetDataTransform(ctx, name)
			if err != nil {
				return nil, backoff.Permanent(fmt.Errorf("getting transform run status: %w", err))
			}

			if dt.LastRunStatus.IsTerminal() {
				return dt, nil
			}

			return dt, fmt.Errorf("transform run status is %v", dt.LastRunStatus)
		},
		backoff.WithBackOff(c.newBackoff(cfg)),
		backoff.WithNotify(func(err error, dur time.Duration) {
			c.GetLogger().DebugContext(ctx, "WaitForTransformRun", slog.Any("err", err), slog.Duration("retry-in", dur))
		}),
	)
}

// RunAndWaitForTransform triggers a transform run and waits for it to complete.
func (c *Client) RunAndWaitForTransform(ctx context.Context, name string, cfg *types.BackoffConfig) (*types.DataTransform, error) {
	if _, err := c.RunDataTransform(ctx, name); err != nil {
		return nil, fmt.Errorf("running transform: %w", err)
	}
	return c.WaitForTransformRun(ctx, name, cfg)
}

// WaitForDLOStatus polls a data lake object until its status matches any of the targets.
func (c *Client) WaitForDLOStatus(ctx context.Context, name string, cfg *types.BackoffConfig, targets ...types.Status) (*types.DataLakeObject, error) {
	return backoff.Retry(
		ctx,
		func() (*types.DataLakeObject, error) {
			dlo, err := c.GetDataLakeObject(ctx, name)
			if err != nil {
				return nil, fmt.Errorf("waiting for DLO status: %w", err)
			}

			if dlo.Status.IsOneOf(targets...) {
				return dlo, nil
			}

			if dlo.Status.IsDeleting() {
				return dlo, backoff.Permanent(fmt.Errorf("DLO is being deleted"))
			}

			return dlo, fmt.Errorf("DLO status is %q, waiting for %v", dlo.Status, targets)
		},
		backoff.WithBackOff(c.newBackoff(cfg)),
		backoff.WithNotify(func(err error, dur time.Duration) {
			c.GetLogger().DebugContext(ctx, "WaitForDLOStatus", slog.Any("err", err), slog.Duration("retry-in", dur))
		}),
	)
}

// WaitForDLODeleted polls until a data lake object returns 404.
func (c *Client) WaitForDLODeleted(ctx context.Context, name string, cfg *types.BackoffConfig) error {
	_, err := backoff.Retry(
		ctx,
		func() (_ struct{}, _ error) {
			_, err := c.GetDataLakeObject(ctx, name)
			if err == nil {
				return struct{}{}, fmt.Errorf("DLO %s still exists", name)
			}
			var sfErr *SalesforceError
			if errors.As(err, &sfErr) && sfErr.IsNotFound() {
				return struct{}{}, nil
			}
			return struct{}{}, fmt.Errorf("waiting for DLO deleted: %w", err)
		},
		backoff.WithBackOff(c.newBackoff(cfg)),
		backoff.WithNotify(func(err error, dur time.Duration) {
			c.GetLogger().DebugContext(ctx, "WaitForDLODeleted", slog.Any("err", err), slog.Duration("retry-in", dur))
		}),
	)
	return err
}

// DeleteDLOAndWait deletes a data lake object and polls until it is gone.
func (c *Client) DeleteDLOAndWait(ctx context.Context, name string, cfg *types.BackoffConfig) error {
	err := c.DeleteDataLakeObject(ctx, name)
	if err != nil {
		var sfErr *SalesforceError
		if errors.As(err, &sfErr) && sfErr.IsNotFound() {
			return nil
		}
		return fmt.Errorf("deleting DLO: %w", err)
	}
	return c.WaitForDLODeleted(ctx, name, cfg)
}

// DeleteTransformAndWait cancels any in-progress run, deletes the transform,
// and polls until it is gone.
func (c *Client) DeleteTransformAndWait(ctx context.Context, name string, cfg *types.BackoffConfig) error {
	// Cancel unconditionally — a cancel on a non-running transform is a noop
	// (may return an error, which we ignore).
	_, _ = c.CancelDataTransform(ctx, name)

	err := c.DeleteDataTransform(ctx, name)
	if err != nil {
		var sfErr *SalesforceError
		if errors.As(err, &sfErr) && sfErr.IsNotFound() {
			return nil
		}
		return fmt.Errorf("deleting transform: %w", err)
	}
	return c.WaitForTransformDeleted(ctx, name, cfg)
}

// ValidateAndCreateTransform validates a transform request, configures the output
// data objects from the validation response, and creates or updates the transform.
// Returns both the created/updated transform and the validation result (for schema inference).
func (c *Client) ValidateAndCreateTransform(ctx context.Context, req *types.DataTransformRequest, primaryKeyFieldName string) (*types.DataTransform, *types.DataTransformValidation, error) {
	validation, err := c.ValidateDataTransform(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("validating transform: %w", err)
	}

	if err := req.ConfigureOutputDataObjects(validation, primaryKeyFieldName); err != nil {
		return nil, nil, err
	}

	dt, err := c.CreateOrUpdateDataTransform(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("creating/updating transform: %w", err)
	}
	return dt, validation, nil
}
