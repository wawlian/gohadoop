// Code generated by protoc-gen-go. DO NOT EDIT.
// source: yarn_server_common_service_protos.proto

/*
Package hadoop_yarn is a generated protocol buffer package.

It is generated from these files:
	yarn_server_common_service_protos.proto

It has these top-level messages:
	ResourceUtilizationRequestProto
	ResourceUtilizationResponseProto
*/
package hadoop_yarn

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type ResourceUtilizationRequestProto struct {
	XXX_unrecognized []byte `json:"-"`
}

func (m *ResourceUtilizationRequestProto) Reset()                    { *m = ResourceUtilizationRequestProto{} }
func (m *ResourceUtilizationRequestProto) String() string            { return proto.CompactTextString(m) }
func (*ResourceUtilizationRequestProto) ProtoMessage()               {}
func (*ResourceUtilizationRequestProto) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

type ResourceUtilizationResponseProto struct {
	NodeStatus       *NodeStatusProto `protobuf:"bytes,1,opt,name=node_status" json:"node_status,omitempty"`
	XXX_unrecognized []byte                        `json:"-"`
}

func (m *ResourceUtilizationResponseProto) Reset()         { *m = ResourceUtilizationResponseProto{} }
func (m *ResourceUtilizationResponseProto) String() string { return proto.CompactTextString(m) }
func (*ResourceUtilizationResponseProto) ProtoMessage()    {}
func (*ResourceUtilizationResponseProto) Descriptor() ([]byte, []int) {
	return fileDescriptor0, []int{1}
}

func (m *ResourceUtilizationResponseProto) GetNodeStatus() *NodeStatusProto {
	if m != nil {
		return m.NodeStatus
	}
	return nil
}

func init() {
	proto.RegisterType((*ResourceUtilizationRequestProto)(nil), "hadoop.yarn.ResourceUtilizationRequestProto")
	proto.RegisterType((*ResourceUtilizationResponseProto)(nil), "hadoop.yarn.ResourceUtilizationResponseProto")
}

func init() { proto.RegisterFile("yarn_server_common_service_protos.proto", fileDescriptor10) }

var fileDescriptor10 = []byte{
	// 196 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x6c, 0x8f, 0xb1, 0x6a, 0xc3, 0x30,
	0x10, 0x40, 0xd1, 0x2a, 0x6f, 0x9e, 0x4a, 0x71, 0xb1, 0xdb, 0xa5, 0x9d, 0x04, 0xed, 0x27, 0xd8,
	0x7b, 0x09, 0x36, 0x1e, 0x32, 0x09, 0x21, 0x1f, 0xb1, 0x20, 0xd6, 0x29, 0x3a, 0x39, 0x90, 0x7c,
	0x41, 0x3e, 0x23, 0x9f, 0x1a, 0x72, 0x5e, 0x1c, 0xf0, 0x74, 0x1c, 0xf7, 0x1e, 0xbc, 0x93, 0xdf,
	0x17, 0x13, 0xbd, 0x26, 0x88, 0x67, 0x88, 0xda, 0xe2, 0x34, 0xe1, 0xb2, 0x39, 0x0b, 0x3a, 0x44,
	0x4c, 0x48, 0x8a, 0x47, 0x9e, 0x8d, 0x66, 0x40, 0x0c, 0xea, 0xc9, 0xbf, 0x97, 0x1b, 0xd6, 0x9a,
	0xfe, 0xfa, 0x94, 0x65, 0x0b, 0x84, 0x73, 0xb4, 0xd0, 0x27, 0x77, 0x74, 0x57, 0x93, 0x1c, 0xfa,
	0x16, 0x4e, 0x33, 0x50, 0xda, 0x31, 0xd2, 0xcb, 0x6a, 0x13, 0xa1, 0x80, 0x9e, 0x80, 0x99, 0xfc,
	0x57, 0x66, 0x1e, 0x07, 0xd0, 0x94, 0x4c, 0x9a, 0xe9, 0x4d, 0x54, 0xe2, 0x27, 0xfb, 0x2b, 0xd4,
	0x2a, 0x45, 0xfd, 0xe3, 0x00, 0x1d, 0x9f, 0x59, 0xa9, 0x1b, 0x59, 0x60, 0x3c, 0x28, 0x13, 0x8c,
	0x1d, 0xe1, 0x85, 0xe4, 0xb2, 0xfa, 0x63, 0x6f, 0xa2, 0xef, 0xb8, 0xbc, 0xe1, 0xf0, 0x6e, 0xf9,
	0x96, 0x6d, 0xba, 0x09, 0x71, 0x17, 0xe2, 0x11, 0x00, 0x00, 0xff, 0xff, 0x06, 0x27, 0x90, 0x0a,
	0x16, 0x01, 0x00, 0x00,
}
