package com.hepo.dfs.namenode.rpc.service;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.29.0)",
    comments = "Source: NameNodeRpcServer.proto")
public final class NameNodeServiceGrpc {

  private NameNodeServiceGrpc() {}

  public static final String SERVICE_NAME = "com.hepo.dfs.namenode.rpc.NameNodeService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.RegisterRequest,
      com.hepo.dfs.namenode.rpc.model.RegisterResponse> getRegisterMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "register",
      requestType = com.hepo.dfs.namenode.rpc.model.RegisterRequest.class,
      responseType = com.hepo.dfs.namenode.rpc.model.RegisterResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.RegisterRequest,
      com.hepo.dfs.namenode.rpc.model.RegisterResponse> getRegisterMethod() {
    io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.RegisterRequest, com.hepo.dfs.namenode.rpc.model.RegisterResponse> getRegisterMethod;
    if ((getRegisterMethod = NameNodeServiceGrpc.getRegisterMethod) == null) {
      synchronized (NameNodeServiceGrpc.class) {
        if ((getRegisterMethod = NameNodeServiceGrpc.getRegisterMethod) == null) {
          NameNodeServiceGrpc.getRegisterMethod = getRegisterMethod =
              io.grpc.MethodDescriptor.<com.hepo.dfs.namenode.rpc.model.RegisterRequest, com.hepo.dfs.namenode.rpc.model.RegisterResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "register"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.hepo.dfs.namenode.rpc.model.RegisterRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.hepo.dfs.namenode.rpc.model.RegisterResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NameNodeServiceMethodDescriptorSupplier("register"))
              .build();
        }
      }
    }
    return getRegisterMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.HeartbeatRequest,
      com.hepo.dfs.namenode.rpc.model.HeartbeatResponse> getHeartbeatMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "heartbeat",
      requestType = com.hepo.dfs.namenode.rpc.model.HeartbeatRequest.class,
      responseType = com.hepo.dfs.namenode.rpc.model.HeartbeatResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.HeartbeatRequest,
      com.hepo.dfs.namenode.rpc.model.HeartbeatResponse> getHeartbeatMethod() {
    io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.HeartbeatRequest, com.hepo.dfs.namenode.rpc.model.HeartbeatResponse> getHeartbeatMethod;
    if ((getHeartbeatMethod = NameNodeServiceGrpc.getHeartbeatMethod) == null) {
      synchronized (NameNodeServiceGrpc.class) {
        if ((getHeartbeatMethod = NameNodeServiceGrpc.getHeartbeatMethod) == null) {
          NameNodeServiceGrpc.getHeartbeatMethod = getHeartbeatMethod =
              io.grpc.MethodDescriptor.<com.hepo.dfs.namenode.rpc.model.HeartbeatRequest, com.hepo.dfs.namenode.rpc.model.HeartbeatResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "heartbeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.hepo.dfs.namenode.rpc.model.HeartbeatRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.hepo.dfs.namenode.rpc.model.HeartbeatResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NameNodeServiceMethodDescriptorSupplier("heartbeat"))
              .build();
        }
      }
    }
    return getHeartbeatMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.MkdirRequest,
      com.hepo.dfs.namenode.rpc.model.MkdirResponse> getMkdirMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "mkdir",
      requestType = com.hepo.dfs.namenode.rpc.model.MkdirRequest.class,
      responseType = com.hepo.dfs.namenode.rpc.model.MkdirResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.MkdirRequest,
      com.hepo.dfs.namenode.rpc.model.MkdirResponse> getMkdirMethod() {
    io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.MkdirRequest, com.hepo.dfs.namenode.rpc.model.MkdirResponse> getMkdirMethod;
    if ((getMkdirMethod = NameNodeServiceGrpc.getMkdirMethod) == null) {
      synchronized (NameNodeServiceGrpc.class) {
        if ((getMkdirMethod = NameNodeServiceGrpc.getMkdirMethod) == null) {
          NameNodeServiceGrpc.getMkdirMethod = getMkdirMethod =
              io.grpc.MethodDescriptor.<com.hepo.dfs.namenode.rpc.model.MkdirRequest, com.hepo.dfs.namenode.rpc.model.MkdirResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "mkdir"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.hepo.dfs.namenode.rpc.model.MkdirRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.hepo.dfs.namenode.rpc.model.MkdirResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NameNodeServiceMethodDescriptorSupplier("mkdir"))
              .build();
        }
      }
    }
    return getMkdirMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.ShutdownRequest,
      com.hepo.dfs.namenode.rpc.model.ShutdownResponse> getShutdownMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "shutdown",
      requestType = com.hepo.dfs.namenode.rpc.model.ShutdownRequest.class,
      responseType = com.hepo.dfs.namenode.rpc.model.ShutdownResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.ShutdownRequest,
      com.hepo.dfs.namenode.rpc.model.ShutdownResponse> getShutdownMethod() {
    io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.ShutdownRequest, com.hepo.dfs.namenode.rpc.model.ShutdownResponse> getShutdownMethod;
    if ((getShutdownMethod = NameNodeServiceGrpc.getShutdownMethod) == null) {
      synchronized (NameNodeServiceGrpc.class) {
        if ((getShutdownMethod = NameNodeServiceGrpc.getShutdownMethod) == null) {
          NameNodeServiceGrpc.getShutdownMethod = getShutdownMethod =
              io.grpc.MethodDescriptor.<com.hepo.dfs.namenode.rpc.model.ShutdownRequest, com.hepo.dfs.namenode.rpc.model.ShutdownResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "shutdown"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.hepo.dfs.namenode.rpc.model.ShutdownRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.hepo.dfs.namenode.rpc.model.ShutdownResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NameNodeServiceMethodDescriptorSupplier("shutdown"))
              .build();
        }
      }
    }
    return getShutdownMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.FetchEditsLogRequest,
      com.hepo.dfs.namenode.rpc.model.FetchEditsLogResponse> getFetchEditsLogMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "fetchEditsLog",
      requestType = com.hepo.dfs.namenode.rpc.model.FetchEditsLogRequest.class,
      responseType = com.hepo.dfs.namenode.rpc.model.FetchEditsLogResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.FetchEditsLogRequest,
      com.hepo.dfs.namenode.rpc.model.FetchEditsLogResponse> getFetchEditsLogMethod() {
    io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.FetchEditsLogRequest, com.hepo.dfs.namenode.rpc.model.FetchEditsLogResponse> getFetchEditsLogMethod;
    if ((getFetchEditsLogMethod = NameNodeServiceGrpc.getFetchEditsLogMethod) == null) {
      synchronized (NameNodeServiceGrpc.class) {
        if ((getFetchEditsLogMethod = NameNodeServiceGrpc.getFetchEditsLogMethod) == null) {
          NameNodeServiceGrpc.getFetchEditsLogMethod = getFetchEditsLogMethod =
              io.grpc.MethodDescriptor.<com.hepo.dfs.namenode.rpc.model.FetchEditsLogRequest, com.hepo.dfs.namenode.rpc.model.FetchEditsLogResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "fetchEditsLog"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.hepo.dfs.namenode.rpc.model.FetchEditsLogRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.hepo.dfs.namenode.rpc.model.FetchEditsLogResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NameNodeServiceMethodDescriptorSupplier("fetchEditsLog"))
              .build();
        }
      }
    }
    return getFetchEditsLogMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest,
      com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> getUpdateCheckpointTxidMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "updateCheckpointTxid",
      requestType = com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest.class,
      responseType = com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest,
      com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> getUpdateCheckpointTxidMethod() {
    io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest, com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> getUpdateCheckpointTxidMethod;
    if ((getUpdateCheckpointTxidMethod = NameNodeServiceGrpc.getUpdateCheckpointTxidMethod) == null) {
      synchronized (NameNodeServiceGrpc.class) {
        if ((getUpdateCheckpointTxidMethod = NameNodeServiceGrpc.getUpdateCheckpointTxidMethod) == null) {
          NameNodeServiceGrpc.getUpdateCheckpointTxidMethod = getUpdateCheckpointTxidMethod =
              io.grpc.MethodDescriptor.<com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest, com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "updateCheckpointTxid"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NameNodeServiceMethodDescriptorSupplier("updateCheckpointTxid"))
              .build();
        }
      }
    }
    return getUpdateCheckpointTxidMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.CreateFileRequest,
      com.hepo.dfs.namenode.rpc.model.CreateFileResponse> getCreateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "create",
      requestType = com.hepo.dfs.namenode.rpc.model.CreateFileRequest.class,
      responseType = com.hepo.dfs.namenode.rpc.model.CreateFileResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.CreateFileRequest,
      com.hepo.dfs.namenode.rpc.model.CreateFileResponse> getCreateMethod() {
    io.grpc.MethodDescriptor<com.hepo.dfs.namenode.rpc.model.CreateFileRequest, com.hepo.dfs.namenode.rpc.model.CreateFileResponse> getCreateMethod;
    if ((getCreateMethod = NameNodeServiceGrpc.getCreateMethod) == null) {
      synchronized (NameNodeServiceGrpc.class) {
        if ((getCreateMethod = NameNodeServiceGrpc.getCreateMethod) == null) {
          NameNodeServiceGrpc.getCreateMethod = getCreateMethod =
              io.grpc.MethodDescriptor.<com.hepo.dfs.namenode.rpc.model.CreateFileRequest, com.hepo.dfs.namenode.rpc.model.CreateFileResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "create"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.hepo.dfs.namenode.rpc.model.CreateFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.hepo.dfs.namenode.rpc.model.CreateFileResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NameNodeServiceMethodDescriptorSupplier("create"))
              .build();
        }
      }
    }
    return getCreateMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static NameNodeServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<NameNodeServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<NameNodeServiceStub>() {
        @Override
        public NameNodeServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new NameNodeServiceStub(channel, callOptions);
        }
      };
    return NameNodeServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static NameNodeServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<NameNodeServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<NameNodeServiceBlockingStub>() {
        @Override
        public NameNodeServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new NameNodeServiceBlockingStub(channel, callOptions);
        }
      };
    return NameNodeServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static NameNodeServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<NameNodeServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<NameNodeServiceFutureStub>() {
        @Override
        public NameNodeServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new NameNodeServiceFutureStub(channel, callOptions);
        }
      };
    return NameNodeServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class NameNodeServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void register(com.hepo.dfs.namenode.rpc.model.RegisterRequest request,
        io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.RegisterResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRegisterMethod(), responseObserver);
    }

    /**
     */
    public void heartbeat(com.hepo.dfs.namenode.rpc.model.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.HeartbeatResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getHeartbeatMethod(), responseObserver);
    }

    /**
     */
    public void mkdir(com.hepo.dfs.namenode.rpc.model.MkdirRequest request,
        io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.MkdirResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getMkdirMethod(), responseObserver);
    }

    /**
     */
    public void shutdown(com.hepo.dfs.namenode.rpc.model.ShutdownRequest request,
        io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.ShutdownResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getShutdownMethod(), responseObserver);
    }

    /**
     */
    public void fetchEditsLog(com.hepo.dfs.namenode.rpc.model.FetchEditsLogRequest request,
        io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.FetchEditsLogResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getFetchEditsLogMethod(), responseObserver);
    }

    /**
     */
    public void updateCheckpointTxid(com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request,
        io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getUpdateCheckpointTxidMethod(), responseObserver);
    }

    /**
     */
    public void create(com.hepo.dfs.namenode.rpc.model.CreateFileRequest request,
        io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.CreateFileResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCreateMethod(), responseObserver);
    }

    @Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getRegisterMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.hepo.dfs.namenode.rpc.model.RegisterRequest,
                com.hepo.dfs.namenode.rpc.model.RegisterResponse>(
                  this, METHODID_REGISTER)))
          .addMethod(
            getHeartbeatMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.hepo.dfs.namenode.rpc.model.HeartbeatRequest,
                com.hepo.dfs.namenode.rpc.model.HeartbeatResponse>(
                  this, METHODID_HEARTBEAT)))
          .addMethod(
            getMkdirMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.hepo.dfs.namenode.rpc.model.MkdirRequest,
                com.hepo.dfs.namenode.rpc.model.MkdirResponse>(
                  this, METHODID_MKDIR)))
          .addMethod(
            getShutdownMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.hepo.dfs.namenode.rpc.model.ShutdownRequest,
                com.hepo.dfs.namenode.rpc.model.ShutdownResponse>(
                  this, METHODID_SHUTDOWN)))
          .addMethod(
            getFetchEditsLogMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.hepo.dfs.namenode.rpc.model.FetchEditsLogRequest,
                com.hepo.dfs.namenode.rpc.model.FetchEditsLogResponse>(
                  this, METHODID_FETCH_EDITS_LOG)))
          .addMethod(
            getUpdateCheckpointTxidMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest,
                com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse>(
                  this, METHODID_UPDATE_CHECKPOINT_TXID)))
          .addMethod(
            getCreateMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.hepo.dfs.namenode.rpc.model.CreateFileRequest,
                com.hepo.dfs.namenode.rpc.model.CreateFileResponse>(
                  this, METHODID_CREATE)))
          .build();
    }
  }

  /**
   */
  public static final class NameNodeServiceStub extends io.grpc.stub.AbstractAsyncStub<NameNodeServiceStub> {
    private NameNodeServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected NameNodeServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new NameNodeServiceStub(channel, callOptions);
    }

    /**
     */
    public void register(com.hepo.dfs.namenode.rpc.model.RegisterRequest request,
        io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.RegisterResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRegisterMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void heartbeat(com.hepo.dfs.namenode.rpc.model.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.HeartbeatResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void mkdir(com.hepo.dfs.namenode.rpc.model.MkdirRequest request,
        io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.MkdirResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getMkdirMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void shutdown(com.hepo.dfs.namenode.rpc.model.ShutdownRequest request,
        io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.ShutdownResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getShutdownMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void fetchEditsLog(com.hepo.dfs.namenode.rpc.model.FetchEditsLogRequest request,
        io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.FetchEditsLogResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getFetchEditsLogMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updateCheckpointTxid(com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request,
        io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getUpdateCheckpointTxidMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void create(com.hepo.dfs.namenode.rpc.model.CreateFileRequest request,
        io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.CreateFileResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCreateMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class NameNodeServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<NameNodeServiceBlockingStub> {
    private NameNodeServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected NameNodeServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new NameNodeServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.hepo.dfs.namenode.rpc.model.RegisterResponse register(com.hepo.dfs.namenode.rpc.model.RegisterRequest request) {
      return blockingUnaryCall(
          getChannel(), getRegisterMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.hepo.dfs.namenode.rpc.model.HeartbeatResponse heartbeat(com.hepo.dfs.namenode.rpc.model.HeartbeatRequest request) {
      return blockingUnaryCall(
          getChannel(), getHeartbeatMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.hepo.dfs.namenode.rpc.model.MkdirResponse mkdir(com.hepo.dfs.namenode.rpc.model.MkdirRequest request) {
      return blockingUnaryCall(
          getChannel(), getMkdirMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.hepo.dfs.namenode.rpc.model.ShutdownResponse shutdown(com.hepo.dfs.namenode.rpc.model.ShutdownRequest request) {
      return blockingUnaryCall(
          getChannel(), getShutdownMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.hepo.dfs.namenode.rpc.model.FetchEditsLogResponse fetchEditsLog(com.hepo.dfs.namenode.rpc.model.FetchEditsLogRequest request) {
      return blockingUnaryCall(
          getChannel(), getFetchEditsLogMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse updateCheckpointTxid(com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request) {
      return blockingUnaryCall(
          getChannel(), getUpdateCheckpointTxidMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.hepo.dfs.namenode.rpc.model.CreateFileResponse create(com.hepo.dfs.namenode.rpc.model.CreateFileRequest request) {
      return blockingUnaryCall(
          getChannel(), getCreateMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class NameNodeServiceFutureStub extends io.grpc.stub.AbstractFutureStub<NameNodeServiceFutureStub> {
    private NameNodeServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected NameNodeServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new NameNodeServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.hepo.dfs.namenode.rpc.model.RegisterResponse> register(
        com.hepo.dfs.namenode.rpc.model.RegisterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRegisterMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.hepo.dfs.namenode.rpc.model.HeartbeatResponse> heartbeat(
        com.hepo.dfs.namenode.rpc.model.HeartbeatRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.hepo.dfs.namenode.rpc.model.MkdirResponse> mkdir(
        com.hepo.dfs.namenode.rpc.model.MkdirRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getMkdirMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.hepo.dfs.namenode.rpc.model.ShutdownResponse> shutdown(
        com.hepo.dfs.namenode.rpc.model.ShutdownRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getShutdownMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.hepo.dfs.namenode.rpc.model.FetchEditsLogResponse> fetchEditsLog(
        com.hepo.dfs.namenode.rpc.model.FetchEditsLogRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getFetchEditsLogMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> updateCheckpointTxid(
        com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getUpdateCheckpointTxidMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.hepo.dfs.namenode.rpc.model.CreateFileResponse> create(
        com.hepo.dfs.namenode.rpc.model.CreateFileRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCreateMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_REGISTER = 0;
  private static final int METHODID_HEARTBEAT = 1;
  private static final int METHODID_MKDIR = 2;
  private static final int METHODID_SHUTDOWN = 3;
  private static final int METHODID_FETCH_EDITS_LOG = 4;
  private static final int METHODID_UPDATE_CHECKPOINT_TXID = 5;
  private static final int METHODID_CREATE = 6;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final NameNodeServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(NameNodeServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_REGISTER:
          serviceImpl.register((com.hepo.dfs.namenode.rpc.model.RegisterRequest) request,
              (io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.RegisterResponse>) responseObserver);
          break;
        case METHODID_HEARTBEAT:
          serviceImpl.heartbeat((com.hepo.dfs.namenode.rpc.model.HeartbeatRequest) request,
              (io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.HeartbeatResponse>) responseObserver);
          break;
        case METHODID_MKDIR:
          serviceImpl.mkdir((com.hepo.dfs.namenode.rpc.model.MkdirRequest) request,
              (io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.MkdirResponse>) responseObserver);
          break;
        case METHODID_SHUTDOWN:
          serviceImpl.shutdown((com.hepo.dfs.namenode.rpc.model.ShutdownRequest) request,
              (io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.ShutdownResponse>) responseObserver);
          break;
        case METHODID_FETCH_EDITS_LOG:
          serviceImpl.fetchEditsLog((com.hepo.dfs.namenode.rpc.model.FetchEditsLogRequest) request,
              (io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.FetchEditsLogResponse>) responseObserver);
          break;
        case METHODID_UPDATE_CHECKPOINT_TXID:
          serviceImpl.updateCheckpointTxid((com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest) request,
              (io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse>) responseObserver);
          break;
        case METHODID_CREATE:
          serviceImpl.create((com.hepo.dfs.namenode.rpc.model.CreateFileRequest) request,
              (io.grpc.stub.StreamObserver<com.hepo.dfs.namenode.rpc.model.CreateFileResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class NameNodeServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    NameNodeServiceBaseDescriptorSupplier() {}

    @Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return NameNodeServer.getDescriptor();
    }

    @Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("NameNodeService");
    }
  }

  private static final class NameNodeServiceFileDescriptorSupplier
      extends NameNodeServiceBaseDescriptorSupplier {
    NameNodeServiceFileDescriptorSupplier() {}
  }

  private static final class NameNodeServiceMethodDescriptorSupplier
      extends NameNodeServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    NameNodeServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (NameNodeServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new NameNodeServiceFileDescriptorSupplier())
              .addMethod(getRegisterMethod())
              .addMethod(getHeartbeatMethod())
              .addMethod(getMkdirMethod())
              .addMethod(getShutdownMethod())
              .addMethod(getFetchEditsLogMethod())
              .addMethod(getUpdateCheckpointTxidMethod())
              .addMethod(getCreateMethod())
              .build();
        }
      }
    }
    return result;
  }
}
