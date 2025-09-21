// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

package lancedb

/*
#cgo CFLAGS: -I${SRCDIR}/../target/generated/include
#cgo LDFLAGS: -L${SRCDIR}/../target/generated/lib -llancedb_go
#include "lancedb.h"
*/
import "C"

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
)

// QueryBuilder provides a fluent interface for building queries
type QueryBuilder struct {
	table   *Table
	filters []string
	limit   int
}

// VectorQueryBuilder extends QueryBuilder for vector similarity searches
type VectorQueryBuilder struct {
	QueryBuilder
	vector []float32
}

// DistanceType represents vector distance metrics
type DistanceType int

const (
	DistanceTypeL2 DistanceType = iota
	DistanceTypeCosine
	DistanceTypeDot
	DistanceTypeHamming
)

// Filter adds a filter condition to the query
func (q *QueryBuilder) Filter(condition string) *QueryBuilder {
	q.filters = append(q.filters, condition)
	return q
}

// Limit sets the maximum number of results to return
func (q *QueryBuilder) Limit(limit int) *QueryBuilder {
	q.limit = limit
	return q
}

// Execute executes the query and returns results
func (q *QueryBuilder) Execute() ([]arrow.Record, error) {
	if q.table.connection.closed {
		return nil, fmt.Errorf("table is closed")
	}

	// This is a placeholder implementation
	// In practice, we'd need to build and execute the query through the Rust layer
	return nil, fmt.Errorf("query execution not yet implemented")
}

// Filter adds a filter condition to the vector query
func (vq *VectorQueryBuilder) Filter(condition string) *VectorQueryBuilder {
	vq.QueryBuilder.Filter(condition)
	return vq
}

// Limit sets the maximum number of results to return
func (vq *VectorQueryBuilder) Limit(limit int) *VectorQueryBuilder {
	vq.QueryBuilder.Limit(limit)
	return vq
}

// DistanceType sets the distance metric for vector search
func (vq *VectorQueryBuilder) DistanceType(distanceType DistanceType) *VectorQueryBuilder {
	// Store distance type for later use
	return vq
}

// Execute executes the vector search query and returns results
func (vq *VectorQueryBuilder) Execute() ([]arrow.Record, error) {
	if vq.table.connection.closed {
		return nil, fmt.Errorf("table is closed")
	}

	// Placeholder implementation - vector query execution not yet fully implemented in C API
	// This is a temporary workaround until the C API types are properly exported
	return nil, fmt.Errorf("vector query execution not yet implemented - C API types need to be fixed")
}

// ExecuteAsync executes the query asynchronously
func (q *QueryBuilder) ExecuteAsync() (<-chan []arrow.Record, <-chan error) {
	resultChan := make(chan []arrow.Record, 1)
	errorChan := make(chan error, 1)

	go func() {
		defer close(resultChan)
		defer close(errorChan)

		results, err := q.Execute()
		if err != nil {
			errorChan <- err
			return
		}

		resultChan <- results
	}()

	return resultChan, errorChan
}

// ExecuteAsync executes the vector query asynchronously
func (vq *VectorQueryBuilder) ExecuteAsync() (<-chan []arrow.Record, <-chan error) {
	resultChan := make(chan []arrow.Record, 1)
	errorChan := make(chan error, 1)

	go func() {
		defer close(resultChan)
		defer close(errorChan)

		results, err := vq.Execute()
		if err != nil {
			errorChan <- err
			return
		}

		resultChan <- results
	}()

	return resultChan, errorChan
}

// QueryOptions provides additional configuration for queries
type QueryOptions struct {
	MaxResults        int
	UseFullPrecision  bool
	BypassVectorIndex bool
}

// ApplyOptions applies query options to the builder
func (q *QueryBuilder) ApplyOptions(options *QueryOptions) *QueryBuilder {
	if options != nil {
		if options.MaxResults > 0 {
			q.Limit(options.MaxResults)
		}
		// Store other options for later use in query execution
	}
	return q
}

// ApplyOptions applies query options to the vector query builder
func (vq *VectorQueryBuilder) ApplyOptions(options *QueryOptions) *VectorQueryBuilder {
	vq.QueryBuilder.ApplyOptions(options)
	return vq
}
