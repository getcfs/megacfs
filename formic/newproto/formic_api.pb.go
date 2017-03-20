// Code generated by protoc-gen-go.
// source: formic_api.proto
// DO NOT EDIT!

/*
Package newproto is a generated protocol buffer package.

It is generated from these files:
	formic_api.proto

It has these top-level messages:
	SetAttrRequest
	SetAttrResponse
	Attr
*/
package newproto

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type SetAttrRequest struct {
	Rpcid uint32 `protobuf:"varint,1,opt,name=rpcid" json:"rpcid,omitempty"`
	Attr  *Attr  `protobuf:"bytes,2,opt,name=attr" json:"attr,omitempty"`
	Valid uint32 `protobuf:"varint,3,opt,name=valid" json:"valid,omitempty"`
}

func (m *SetAttrRequest) Reset()                    { *m = SetAttrRequest{} }
func (m *SetAttrRequest) String() string            { return proto.CompactTextString(m) }
func (*SetAttrRequest) ProtoMessage()               {}
func (*SetAttrRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *SetAttrRequest) GetRpcid() uint32 {
	if m != nil {
		return m.Rpcid
	}
	return 0
}

func (m *SetAttrRequest) GetAttr() *Attr {
	if m != nil {
		return m.Attr
	}
	return nil
}

func (m *SetAttrRequest) GetValid() uint32 {
	if m != nil {
		return m.Valid
	}
	return 0
}

type SetAttrResponse struct {
	Rpcid uint32 `protobuf:"varint,1,opt,name=rpcid" json:"rpcid,omitempty"`
	Attr  *Attr  `protobuf:"bytes,2,opt,name=attr" json:"attr,omitempty"`
	Err   string `protobuf:"bytes,3,opt,name=err" json:"err,omitempty"`
}

func (m *SetAttrResponse) Reset()                    { *m = SetAttrResponse{} }
func (m *SetAttrResponse) String() string            { return proto.CompactTextString(m) }
func (*SetAttrResponse) ProtoMessage()               {}
func (*SetAttrResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *SetAttrResponse) GetRpcid() uint32 {
	if m != nil {
		return m.Rpcid
	}
	return 0
}

func (m *SetAttrResponse) GetAttr() *Attr {
	if m != nil {
		return m.Attr
	}
	return nil
}

func (m *SetAttrResponse) GetErr() string {
	if m != nil {
		return m.Err
	}
	return ""
}

type Attr struct {
	Inode  uint64 `protobuf:"varint,1,opt,name=inode" json:"inode,omitempty"`
	Atime  int64  `protobuf:"varint,2,opt,name=atime" json:"atime,omitempty"`
	Mtime  int64  `protobuf:"varint,3,opt,name=mtime" json:"mtime,omitempty"`
	Ctime  int64  `protobuf:"varint,4,opt,name=ctime" json:"ctime,omitempty"`
	Crtime int64  `protobuf:"varint,5,opt,name=crtime" json:"crtime,omitempty"`
	Mode   uint32 `protobuf:"varint,6,opt,name=mode" json:"mode,omitempty"`
	Valid  int32  `protobuf:"varint,7,opt,name=valid" json:"valid,omitempty"`
	Size   uint64 `protobuf:"varint,8,opt,name=size" json:"size,omitempty"`
	Uid    uint32 `protobuf:"varint,9,opt,name=uid" json:"uid,omitempty"`
	Gid    uint32 `protobuf:"varint,10,opt,name=gid" json:"gid,omitempty"`
}

func (m *Attr) Reset()                    { *m = Attr{} }
func (m *Attr) String() string            { return proto.CompactTextString(m) }
func (*Attr) ProtoMessage()               {}
func (*Attr) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

func (m *Attr) GetInode() uint64 {
	if m != nil {
		return m.Inode
	}
	return 0
}

func (m *Attr) GetAtime() int64 {
	if m != nil {
		return m.Atime
	}
	return 0
}

func (m *Attr) GetMtime() int64 {
	if m != nil {
		return m.Mtime
	}
	return 0
}

func (m *Attr) GetCtime() int64 {
	if m != nil {
		return m.Ctime
	}
	return 0
}

func (m *Attr) GetCrtime() int64 {
	if m != nil {
		return m.Crtime
	}
	return 0
}

func (m *Attr) GetMode() uint32 {
	if m != nil {
		return m.Mode
	}
	return 0
}

func (m *Attr) GetValid() int32 {
	if m != nil {
		return m.Valid
	}
	return 0
}

func (m *Attr) GetSize() uint64 {
	if m != nil {
		return m.Size
	}
	return 0
}

func (m *Attr) GetUid() uint32 {
	if m != nil {
		return m.Uid
	}
	return 0
}

func (m *Attr) GetGid() uint32 {
	if m != nil {
		return m.Gid
	}
	return 0
}

func init() {
	proto.RegisterType((*SetAttrRequest)(nil), "newproto.SetAttrRequest")
	proto.RegisterType((*SetAttrResponse)(nil), "newproto.SetAttrResponse")
	proto.RegisterType((*Attr)(nil), "newproto.Attr")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for Formic service

type FormicClient interface {
	SetAttr(ctx context.Context, opts ...grpc.CallOption) (Formic_SetAttrClient, error)
}

type formicClient struct {
	cc *grpc.ClientConn
}

func NewFormicClient(cc *grpc.ClientConn) FormicClient {
	return &formicClient{cc}
}

func (c *formicClient) SetAttr(ctx context.Context, opts ...grpc.CallOption) (Formic_SetAttrClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_Formic_serviceDesc.Streams[0], c.cc, "/newproto.Formic/SetAttr", opts...)
	if err != nil {
		return nil, err
	}
	x := &formicSetAttrClient{stream}
	return x, nil
}

type Formic_SetAttrClient interface {
	Send(*SetAttrRequest) error
	Recv() (*SetAttrResponse, error)
	grpc.ClientStream
}

type formicSetAttrClient struct {
	grpc.ClientStream
}

func (x *formicSetAttrClient) Send(m *SetAttrRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *formicSetAttrClient) Recv() (*SetAttrResponse, error) {
	m := new(SetAttrResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// Server API for Formic service

type FormicServer interface {
	SetAttr(Formic_SetAttrServer) error
}

func RegisterFormicServer(s *grpc.Server, srv FormicServer) {
	s.RegisterService(&_Formic_serviceDesc, srv)
}

func _Formic_SetAttr_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(FormicServer).SetAttr(&formicSetAttrServer{stream})
}

type Formic_SetAttrServer interface {
	Send(*SetAttrResponse) error
	Recv() (*SetAttrRequest, error)
	grpc.ServerStream
}

type formicSetAttrServer struct {
	grpc.ServerStream
}

func (x *formicSetAttrServer) Send(m *SetAttrResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *formicSetAttrServer) Recv() (*SetAttrRequest, error) {
	m := new(SetAttrRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var _Formic_serviceDesc = grpc.ServiceDesc{
	ServiceName: "newproto.Formic",
	HandlerType: (*FormicServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "SetAttr",
			Handler:       _Formic_SetAttr_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "formic_api.proto",
}

func init() { proto.RegisterFile("formic_api.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 289 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xa4, 0x91, 0xcf, 0x4a, 0xf4, 0x30,
	0x14, 0xc5, 0xbf, 0x7c, 0xfd, 0x33, 0x33, 0x57, 0x1c, 0x87, 0x20, 0x12, 0x5d, 0x95, 0xac, 0xba,
	0x2a, 0x32, 0x3e, 0xc1, 0x80, 0xb8, 0x74, 0x11, 0xd7, 0xa2, 0xb5, 0x89, 0xc3, 0x05, 0xd3, 0xd4,
	0x34, 0xa3, 0xe0, 0x93, 0xfa, 0x38, 0x92, 0x9b, 0x8e, 0x83, 0xe0, 0xce, 0xdd, 0x39, 0xbf, 0xf4,
	0xde, 0xd3, 0x9c, 0xc0, 0xea, 0xd9, 0x79, 0x8b, 0xdd, 0x43, 0x3b, 0x60, 0x33, 0x78, 0x17, 0x1c,
	0x9f, 0xf7, 0xe6, 0x9d, 0x94, 0x7c, 0x84, 0xe5, 0x9d, 0x09, 0x9b, 0x10, 0xbc, 0x32, 0xaf, 0x3b,
	0x33, 0x06, 0x7e, 0x0a, 0x85, 0x1f, 0x3a, 0xd4, 0x82, 0x55, 0xac, 0x3e, 0x56, 0xc9, 0x70, 0x09,
	0x79, 0x1b, 0x82, 0x17, 0xff, 0x2b, 0x56, 0x1f, 0xad, 0x97, 0xcd, 0x7e, 0x41, 0x43, 0xa3, 0x74,
	0x16, 0x27, 0xdf, 0xda, 0x17, 0xd4, 0x22, 0x4b, 0x93, 0x64, 0xe4, 0x3d, 0x9c, 0x7c, 0x27, 0x8c,
	0x83, 0xeb, 0x47, 0xf3, 0x87, 0x88, 0x15, 0x64, 0xc6, 0x7b, 0x0a, 0x58, 0xa8, 0x28, 0xe5, 0x27,
	0x83, 0x7c, 0x33, 0xa5, 0x63, 0xef, 0xb4, 0xa1, 0xa5, 0xb9, 0x4a, 0x26, 0xd2, 0x36, 0xa0, 0x35,
	0xb4, 0x35, 0x53, 0xc9, 0x44, 0x6a, 0x89, 0x66, 0x89, 0xda, 0x3d, 0xed, 0x88, 0xe6, 0x89, 0x92,
	0xe1, 0x67, 0x50, 0x76, 0x9e, 0x70, 0x41, 0x78, 0x72, 0x9c, 0x43, 0x6e, 0x63, 0x5c, 0x49, 0x77,
	0x20, 0x7d, 0x68, 0x60, 0x56, 0xb1, 0xba, 0x98, 0x1a, 0x88, 0x5f, 0x8e, 0xf8, 0x61, 0xc4, 0x9c,
	0x7e, 0x8c, 0x74, 0xbc, 0xc8, 0x0e, 0xb5, 0x58, 0xd0, 0x70, 0x94, 0x91, 0x6c, 0x51, 0x0b, 0x48,
	0x64, 0x8b, 0x7a, 0x7d, 0x0b, 0xe5, 0x0d, 0xbd, 0x1c, 0xbf, 0x86, 0xd9, 0xd4, 0x21, 0x17, 0x87,
	0x5e, 0x7e, 0x3e, 0xdc, 0xc5, 0xf9, 0x2f, 0x27, 0xa9, 0x70, 0xf9, 0xaf, 0x66, 0x97, 0xec, 0xa9,
	0xa4, 0xc3, 0xab, 0xaf, 0x00, 0x00, 0x00, 0xff, 0xff, 0xcb, 0x6b, 0x4a, 0xb7, 0x10, 0x02, 0x00,
	0x00,
}