// Copyright (c) 2022-present PocketBase contributors.
// SPDX-License-Identifier: MIT
//
// Vendored from https://github.com/pocketbase/pocketbase/tree/master/tools/cron.
// The MIT license text is available at https://opensource.org/licenses/MIT.

package cron

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

// Cron is a crontab-like struct for tasks/jobs scheduling.
type Cron struct {
	timezone *time.Location
	ticker   *time.Ticker
	jobs     []*Job
	interval time.Duration
	stopCh   chan struct{}
	running  bool
	mux      sync.RWMutex
}

// New create a new Cron struct with default tick interval of 1 minute
// and timezone in UTC.
func New() *Cron {
	return &Cron{
		interval: 1 * time.Minute,
		timezone: time.UTC,
		jobs:     []*Job{},
	}
}

// SetInterval changes the current cron tick interval.
func (c *Cron) SetInterval(d time.Duration) {
	c.mux.Lock()
	wasStarted := c.running
	c.interval = d
	c.mux.Unlock()

	if wasStarted {
		c.Start()
	}
}

// SetTimezone changes the current cron tick timezone.
func (c *Cron) SetTimezone(l *time.Location) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.timezone = l
}

// MustAdd is similar to Add() but panic on failure.
func (c *Cron) MustAdd(jobId string, cronExpr string, run func()) {
	if err := c.Add(jobId, cronExpr, run); err != nil {
		panic(err)
	}
}

// Add registers a single cron job. If a job with the provided id already
// exists, it is replaced.
func (c *Cron) Add(jobId string, cronExpr string, fn func()) error {
	if fn == nil {
		return errors.New("failed to add new cron job: fn must be non-nil function")
	}

	schedule, err := NewSchedule(cronExpr)
	if err != nil {
		return fmt.Errorf("failed to add new cron job: %w", err)
	}

	c.mux.Lock()
	defer c.mux.Unlock()

	c.jobs = slices.DeleteFunc(c.jobs, func(j *Job) bool {
		return j.Id() == jobId
	})

	c.jobs = append(c.jobs, &Job{
		id:       jobId,
		fn:       fn,
		schedule: schedule,
	})

	return nil
}

// Remove removes a single cron job by its id.
func (c *Cron) Remove(jobId string) {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.jobs == nil {
		return
	}

	c.jobs = slices.DeleteFunc(c.jobs, func(j *Job) bool {
		return j.Id() == jobId
	})
}

// RemoveAll removes all registered cron jobs.
func (c *Cron) RemoveAll() {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.jobs = []*Job{}
}

// Total returns the current total number of registered cron jobs.
func (c *Cron) Total() int {
	c.mux.RLock()
	defer c.mux.RUnlock()
	return len(c.jobs)
}

// Jobs returns a shallow copy of the currently registered cron jobs.
func (c *Cron) Jobs() []*Job {
	c.mux.RLock()
	defer c.mux.RUnlock()

	out := make([]*Job, len(c.jobs))
	copy(out, c.jobs)
	return out
}

// Stop stops the current cron ticker (if not already).
func (c *Cron) Stop() {
	c.mux.Lock()
	if !c.running {
		c.mux.Unlock()
		return
	}

	stopCh := c.stopCh
	ticker := c.ticker
	c.stopCh = nil
	c.ticker = nil
	c.running = false
	c.mux.Unlock()

	if ticker != nil {
		ticker.Stop()
	}

	if stopCh != nil {
		close(stopCh)
	}
}

// Start starts the cron ticker.
func (c *Cron) Start() {
	c.Stop()

	c.mux.Lock()
	interval := c.interval
	stopCh := make(chan struct{})
	c.stopCh = stopCh
	c.running = true
	c.mux.Unlock()

	go c.runLoop(interval, stopCh)
}

// HasStarted reports whether the cron ticker has been started.
func (c *Cron) HasStarted() bool {
	c.mux.RLock()
	defer c.mux.RUnlock()
	return c.running
}

// RunDue runs all jobs that are due at t. Exposed for tests that need to
// drive the scheduler deterministically without sleeping.
func (c *Cron) RunDue(t time.Time) {
	c.runDue(t)
}

func (c *Cron) runDue(t time.Time) {
	c.mux.RLock()
	timezone := c.timezone
	jobs := make([]*Job, len(c.jobs))
	copy(jobs, c.jobs)
	c.mux.RUnlock()

	moment := NewMoment(t.In(timezone))

	for _, j := range jobs {
		if j.schedule.IsDue(moment) {
			go j.Run()
		}
	}
}

func (c *Cron) runLoop(interval time.Duration, stopCh <-chan struct{}) {
	now := time.Now()
	next := now.Add(interval).Truncate(interval)
	delay := next.Sub(now)

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-stopCh:
		return
	case <-timer.C:
	}

	c.runDue(time.Now())

	ticker := time.NewTicker(interval)

	c.mux.Lock()
	if c.stopCh != stopCh || !c.running {
		c.mux.Unlock()
		ticker.Stop()
		return
	}
	c.ticker = ticker
	c.mux.Unlock()

	defer func() {
		ticker.Stop()

		c.mux.Lock()
		if c.stopCh == stopCh {
			c.ticker = nil
			c.running = false
			c.stopCh = nil
		}
		c.mux.Unlock()
	}()

	for {
		select {
		case <-stopCh:
			return
		case t := <-ticker.C:
			c.runDue(t)
		}
	}
}
