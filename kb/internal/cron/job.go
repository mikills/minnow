// Copyright (c) 2022-present PocketBase contributors.
// SPDX-License-Identifier: MIT
//
// Vendored from https://github.com/pocketbase/pocketbase/tree/master/tools/cron.

package cron

// Job defines a single registered cron job.
type Job struct {
	fn       func()
	schedule *Schedule
	id       string
}

// Id returns the cron job id.
func (j *Job) Id() string { return j.id }

// Expression returns the plain cron job schedule expression.
func (j *Job) Expression() string { return j.schedule.rawExpr }

// Run runs the cron job function.
func (j *Job) Run() {
	if j.fn != nil {
		j.fn()
	}
}
