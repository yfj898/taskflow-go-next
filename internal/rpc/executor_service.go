package rpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

const ExecutorServiceName = "taskflow.executor.v1.ExecutorService"

type ExecutorServiceClient interface {
	Execute(ctx context.Context, in *structpb.Struct, opts ...grpc.CallOption) (*structpb.Struct, error)
}

type executorServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewExecutorServiceClient(cc grpc.ClientConnInterface) ExecutorServiceClient {
	return &executorServiceClient{cc: cc}
}

func (c *executorServiceClient) Execute(ctx context.Context, in *structpb.Struct, opts ...grpc.CallOption) (*structpb.Struct, error) {
	out := new(structpb.Struct)
	err := c.cc.Invoke(ctx, "/"+ExecutorServiceName+"/Execute", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type ExecutorServiceServer interface {
	Execute(context.Context, *structpb.Struct) (*structpb.Struct, error)
}

func RegisterExecutorServiceServer(reg grpc.ServiceRegistrar, srv ExecutorServiceServer) {
	reg.RegisterService(&ExecutorService_ServiceDesc, srv)
}

func _ExecutorService_Execute_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(structpb.Struct)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutorServiceServer).Execute(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/" + ExecutorServiceName + "/Execute"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(ExecutorServiceServer).Execute(ctx, req.(*structpb.Struct))
	}
	return interceptor(ctx, in, info, handler)
}

var ExecutorService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: ExecutorServiceName,
	HandlerType: (*ExecutorServiceServer)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Execute",
		Handler:    _ExecutorService_Execute_Handler,
	}},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/executor.proto",
}
