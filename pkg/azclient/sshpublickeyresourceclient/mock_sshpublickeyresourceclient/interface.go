// /*
// Copyright The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// */
//

// Code generated by MockGen. DO NOT EDIT.
// Source: sshpublickeyresourceclient/interface.go
//
// Generated by this command:
//
//	mockgen -package mock_sshpublickeyresourceclient -source sshpublickeyresourceclient/interface.go -typed -write_generate_directive -copyright_file ../../hack/boilerplate/boilerplate.generatego.txt
//

// Package mock_sshpublickeyresourceclient is a generated GoMock package.
package mock_sshpublickeyresourceclient

import (
	context "context"
	reflect "reflect"

	armcompute "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v6"
	gomock "go.uber.org/mock/gomock"
)

//go:generate mockgen -package mock_sshpublickeyresourceclient -source sshpublickeyresourceclient/interface.go -typed -write_generate_directive -copyright_file ../../hack/boilerplate/boilerplate.generatego.txt

// MockInterface is a mock of Interface interface.
type MockInterface struct {
	ctrl     *gomock.Controller
	recorder *MockInterfaceMockRecorder
	isgomock struct{}
}

// MockInterfaceMockRecorder is the mock recorder for MockInterface.
type MockInterfaceMockRecorder struct {
	mock *MockInterface
}

// NewMockInterface creates a new mock instance.
func NewMockInterface(ctrl *gomock.Controller) *MockInterface {
	mock := &MockInterface{ctrl: ctrl}
	mock.recorder = &MockInterfaceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockInterface) EXPECT() *MockInterfaceMockRecorder {
	return m.recorder
}

// Create mocks base method.
func (m *MockInterface) Create(ctx context.Context, resourceGroupName, sshPublicKeyName string, parameters armcompute.SSHPublicKeyResource) (*armcompute.SSHPublicKeyResource, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Create", ctx, resourceGroupName, sshPublicKeyName, parameters)
	ret0, _ := ret[0].(*armcompute.SSHPublicKeyResource)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Create indicates an expected call of Create.
func (mr *MockInterfaceMockRecorder) Create(ctx, resourceGroupName, sshPublicKeyName, parameters any) *MockInterfaceCreateCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Create", reflect.TypeOf((*MockInterface)(nil).Create), ctx, resourceGroupName, sshPublicKeyName, parameters)
	return &MockInterfaceCreateCall{Call: call}
}

// MockInterfaceCreateCall wrap *gomock.Call
type MockInterfaceCreateCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockInterfaceCreateCall) Return(arg0 *armcompute.SSHPublicKeyResource, arg1 error) *MockInterfaceCreateCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockInterfaceCreateCall) Do(f func(context.Context, string, string, armcompute.SSHPublicKeyResource) (*armcompute.SSHPublicKeyResource, error)) *MockInterfaceCreateCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockInterfaceCreateCall) DoAndReturn(f func(context.Context, string, string, armcompute.SSHPublicKeyResource) (*armcompute.SSHPublicKeyResource, error)) *MockInterfaceCreateCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Delete mocks base method.
func (m *MockInterface) Delete(ctx context.Context, resourceGroupName, resourceName string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", ctx, resourceGroupName, resourceName)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockInterfaceMockRecorder) Delete(ctx, resourceGroupName, resourceName any) *MockInterfaceDeleteCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockInterface)(nil).Delete), ctx, resourceGroupName, resourceName)
	return &MockInterfaceDeleteCall{Call: call}
}

// MockInterfaceDeleteCall wrap *gomock.Call
type MockInterfaceDeleteCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockInterfaceDeleteCall) Return(arg0 error) *MockInterfaceDeleteCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockInterfaceDeleteCall) Do(f func(context.Context, string, string) error) *MockInterfaceDeleteCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockInterfaceDeleteCall) DoAndReturn(f func(context.Context, string, string) error) *MockInterfaceDeleteCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// GenerateKeyPair mocks base method.
func (m *MockInterface) GenerateKeyPair(ctx context.Context, resourceGroupName, sshPublicKeyName string) (*armcompute.SSHPublicKeyGenerateKeyPairResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateKeyPair", ctx, resourceGroupName, sshPublicKeyName)
	ret0, _ := ret[0].(*armcompute.SSHPublicKeyGenerateKeyPairResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GenerateKeyPair indicates an expected call of GenerateKeyPair.
func (mr *MockInterfaceMockRecorder) GenerateKeyPair(ctx, resourceGroupName, sshPublicKeyName any) *MockInterfaceGenerateKeyPairCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateKeyPair", reflect.TypeOf((*MockInterface)(nil).GenerateKeyPair), ctx, resourceGroupName, sshPublicKeyName)
	return &MockInterfaceGenerateKeyPairCall{Call: call}
}

// MockInterfaceGenerateKeyPairCall wrap *gomock.Call
type MockInterfaceGenerateKeyPairCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockInterfaceGenerateKeyPairCall) Return(arg0 *armcompute.SSHPublicKeyGenerateKeyPairResult, arg1 error) *MockInterfaceGenerateKeyPairCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockInterfaceGenerateKeyPairCall) Do(f func(context.Context, string, string) (*armcompute.SSHPublicKeyGenerateKeyPairResult, error)) *MockInterfaceGenerateKeyPairCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockInterfaceGenerateKeyPairCall) DoAndReturn(f func(context.Context, string, string) (*armcompute.SSHPublicKeyGenerateKeyPairResult, error)) *MockInterfaceGenerateKeyPairCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Get mocks base method.
func (m *MockInterface) Get(ctx context.Context, resourceGroupName, resourceName string) (*armcompute.SSHPublicKeyResource, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", ctx, resourceGroupName, resourceName)
	ret0, _ := ret[0].(*armcompute.SSHPublicKeyResource)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockInterfaceMockRecorder) Get(ctx, resourceGroupName, resourceName any) *MockInterfaceGetCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockInterface)(nil).Get), ctx, resourceGroupName, resourceName)
	return &MockInterfaceGetCall{Call: call}
}

// MockInterfaceGetCall wrap *gomock.Call
type MockInterfaceGetCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockInterfaceGetCall) Return(result *armcompute.SSHPublicKeyResource, rerr error) *MockInterfaceGetCall {
	c.Call = c.Call.Return(result, rerr)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockInterfaceGetCall) Do(f func(context.Context, string, string) (*armcompute.SSHPublicKeyResource, error)) *MockInterfaceGetCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockInterfaceGetCall) DoAndReturn(f func(context.Context, string, string) (*armcompute.SSHPublicKeyResource, error)) *MockInterfaceGetCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// List mocks base method.
func (m *MockInterface) List(ctx context.Context, resourceGroupName string) ([]*armcompute.SSHPublicKeyResource, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "List", ctx, resourceGroupName)
	ret0, _ := ret[0].([]*armcompute.SSHPublicKeyResource)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// List indicates an expected call of List.
func (mr *MockInterfaceMockRecorder) List(ctx, resourceGroupName any) *MockInterfaceListCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "List", reflect.TypeOf((*MockInterface)(nil).List), ctx, resourceGroupName)
	return &MockInterfaceListCall{Call: call}
}

// MockInterfaceListCall wrap *gomock.Call
type MockInterfaceListCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockInterfaceListCall) Return(result []*armcompute.SSHPublicKeyResource, rerr error) *MockInterfaceListCall {
	c.Call = c.Call.Return(result, rerr)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockInterfaceListCall) Do(f func(context.Context, string) ([]*armcompute.SSHPublicKeyResource, error)) *MockInterfaceListCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockInterfaceListCall) DoAndReturn(f func(context.Context, string) ([]*armcompute.SSHPublicKeyResource, error)) *MockInterfaceListCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Update mocks base method.
func (m *MockInterface) Update(ctx context.Context, resourceGroupName, sshPublicKeyName string, parameters armcompute.SSHPublicKeyUpdateResource) (*armcompute.SSHPublicKeyResource, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Update", ctx, resourceGroupName, sshPublicKeyName, parameters)
	ret0, _ := ret[0].(*armcompute.SSHPublicKeyResource)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Update indicates an expected call of Update.
func (mr *MockInterfaceMockRecorder) Update(ctx, resourceGroupName, sshPublicKeyName, parameters any) *MockInterfaceUpdateCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Update", reflect.TypeOf((*MockInterface)(nil).Update), ctx, resourceGroupName, sshPublicKeyName, parameters)
	return &MockInterfaceUpdateCall{Call: call}
}

// MockInterfaceUpdateCall wrap *gomock.Call
type MockInterfaceUpdateCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockInterfaceUpdateCall) Return(arg0 *armcompute.SSHPublicKeyResource, arg1 error) *MockInterfaceUpdateCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockInterfaceUpdateCall) Do(f func(context.Context, string, string, armcompute.SSHPublicKeyUpdateResource) (*armcompute.SSHPublicKeyResource, error)) *MockInterfaceUpdateCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockInterfaceUpdateCall) DoAndReturn(f func(context.Context, string, string, armcompute.SSHPublicKeyUpdateResource) (*armcompute.SSHPublicKeyResource, error)) *MockInterfaceUpdateCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}
