/*
 * serialization_test_kit_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package testkit

import (
	"encoding/json"
	"testing"
)

type testMessage struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

type jsonSerializer struct{}

func (j *jsonSerializer) Identifier() int32                { return 100 }
func (j *jsonSerializer) Manifest(obj any) string          { return "test-message" }
func (j *jsonSerializer) Serialize(obj any) ([]byte, error) {
	return json.Marshal(obj)
}
func (j *jsonSerializer) Deserialize(data []byte, manifest string) (any, error) {
	var msg testMessage
	err := json.Unmarshal(data, &msg)
	return msg, err
}

func TestSerializationTestKit_RoundTrip(t *testing.T) {
	reg := NewSimpleSerializerRegistry()
	ser := &jsonSerializer{}
	reg.Register(testMessage{}, ser)

	stk := NewSerializationTestKit(reg)
	stk.VerifySerialization(t, testMessage{Name: "hello", Value: 42})
}

func TestSerializationTestKit_SpecificSerializer(t *testing.T) {
	reg := NewSimpleSerializerRegistry()
	ser := &jsonSerializer{}

	stk := NewSerializationTestKit(reg)
	stk.VerifySerializationOf(t, testMessage{Name: "test", Value: 7}, ser)
}
