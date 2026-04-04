/*
 * retry_flow.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import "time"

// ─── RetryFlowWithBackoff ─────────────────────────────────────────────────────

// retryFlowIterator processes each upstream element through innerFlow, retrying
// on error with exponential backoff.
type retryFlowIterator[In, Out any] struct {
	settings RestartSettings
	flow     Flow[In, Out, NotUsed]
	upstream iterator[In]
}

func (r *retryFlowIterator[In, Out]) next() (Out, bool, error) {
	elem, ok, err := r.upstream.next()
	if !ok || err != nil {
		var zero Out
		return zero, ok, err
	}

	backoff := r.settings.MinBackoff
	for attempt := 0; ; attempt++ {
		// Run this single element through the inner flow.
		single := &sliceIterator[In]{elems: []In{elem}}
		flowIter, _ := r.flow.attach(single)
		result, ok2, flowErr := flowIter.next()

		if flowErr == nil {
			// Either a valid result or a clean (unexpected) completion.
			return result, ok2, nil
		}

		// Flow failed for this element.
		if r.settings.MaxRestarts >= 0 && attempt >= r.settings.MaxRestarts {
			// Budget exhausted — propagate the error.
			var zero Out
			return zero, false, flowErr
		}

		time.Sleep(backoff)
		backoff = calcBackoff(backoff, r.settings)
	}
}

// RetryFlowWithBackoff creates a Flow that processes each element through
// innerFlow, retrying failed elements with exponential backoff.
//
// For every upstream element the iterator attaches innerFlow to a single-element
// source and pulls one result.  If innerFlow signals an error for that element
// and the retry budget (settings.MaxRestarts) has not been exhausted, the
// element is re-submitted after a backoff delay.  Once the budget is exhausted
// the error propagates downstream.
//
// Backoff starts at settings.MinBackoff, doubles on each retry up to
// settings.MaxBackoff, and is jittered by settings.RandomFactor.
//
// Use MaxRestarts = -1 for unlimited per-element retries.
//
// Mirroring Pekko's RetryFlow.withBackoff.
//
// Example — retry a flaky HTTP lookup up to 3 times per element:
//
//	lookup := stream.MapE(func(id string) (Result, error) { return fetch(id) })
//	retrying := stream.RetryFlowWithBackoff(
//	    stream.RestartSettings{
//	        MinBackoff:   50 * time.Millisecond,
//	        MaxBackoff:   2 * time.Second,
//	        RandomFactor: 0.2,
//	        MaxRestarts:  3,
//	    },
//	    lookup,
//	)
func RetryFlowWithBackoff[In, Out any](
	settings RestartSettings,
	innerFlow Flow[In, Out, NotUsed],
) Flow[In, Out, NotUsed] {
	return Flow[In, Out, NotUsed]{
		attach: func(upstream iterator[In]) (iterator[Out], NotUsed) {
			return &retryFlowIterator[In, Out]{
				settings: settings,
				flow:     innerFlow,
				upstream: upstream,
			}, NotUsed{}
		},
	}
}
