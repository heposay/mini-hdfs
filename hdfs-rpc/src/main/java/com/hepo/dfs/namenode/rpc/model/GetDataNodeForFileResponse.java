// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: NameNodeRpcModel.proto

package com.hepo.dfs.namenode.rpc.model;

/**
 * Protobuf type {@code com.hepo.dfs.namenode.rpc.GetDataNodeForFileResponse}
 */
public  final class GetDataNodeForFileResponse extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:com.hepo.dfs.namenode.rpc.GetDataNodeForFileResponse)
    GetDataNodeForFileResponseOrBuilder {
private static final long serialVersionUID = 0L;
  // Use GetDataNodeForFileResponse.newBuilder() to construct.
  private GetDataNodeForFileResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private GetDataNodeForFileResponse() {
    dataNodeInfo_ = "";
  }

  @Override
  @SuppressWarnings({"unused"})
  protected Object newInstance(
      UnusedPrivateParameter unused) {
    return new GetDataNodeForFileResponse();
  }

  @Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private GetDataNodeForFileResponse(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    if (extensionRegistry == null) {
      throw new NullPointerException();
    }
    com.google.protobuf.UnknownFieldSet.Builder unknownFields =
        com.google.protobuf.UnknownFieldSet.newBuilder();
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          case 10: {
            String s = input.readStringRequireUtf8();

            dataNodeInfo_ = s;
            break;
          }
          default: {
            if (!parseUnknownField(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return NameNodeRpcModel.internal_static_com_hepo_dfs_namenode_rpc_GetDataNodeForFileResponse_descriptor;
  }

  @Override
  protected FieldAccessorTable
      internalGetFieldAccessorTable() {
    return NameNodeRpcModel.internal_static_com_hepo_dfs_namenode_rpc_GetDataNodeForFileResponse_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            GetDataNodeForFileResponse.class, Builder.class);
  }

  public static final int DATANODEINFO_FIELD_NUMBER = 1;
  private volatile Object dataNodeInfo_;
  /**
   * <code>string dataNodeInfo = 1;</code>
   * @return The dataNodeInfo.
   */
  public String getDataNodeInfo() {
    Object ref = dataNodeInfo_;
    if (ref instanceof String) {
      return (String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      String s = bs.toStringUtf8();
      dataNodeInfo_ = s;
      return s;
    }
  }
  /**
   * <code>string dataNodeInfo = 1;</code>
   * @return The bytes for dataNodeInfo.
   */
  public com.google.protobuf.ByteString
      getDataNodeInfoBytes() {
    Object ref = dataNodeInfo_;
    if (ref instanceof String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (String) ref);
      dataNodeInfo_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  private byte memoizedIsInitialized = -1;
  @Override
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  @Override
  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (!getDataNodeInfoBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, dataNodeInfo_);
    }
    unknownFields.writeTo(output);
  }

  @Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (!getDataNodeInfoBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, dataNodeInfo_);
    }
    size += unknownFields.getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof GetDataNodeForFileResponse)) {
      return super.equals(obj);
    }
    GetDataNodeForFileResponse other = (GetDataNodeForFileResponse) obj;

    if (!getDataNodeInfo()
        .equals(other.getDataNodeInfo())) return false;
    if (!unknownFields.equals(other.unknownFields)) return false;
    return true;
  }

  @Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    hash = (37 * hash) + DATANODEINFO_FIELD_NUMBER;
    hash = (53 * hash) + getDataNodeInfo().hashCode();
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static GetDataNodeForFileResponse parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static GetDataNodeForFileResponse parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static GetDataNodeForFileResponse parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static GetDataNodeForFileResponse parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static GetDataNodeForFileResponse parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static GetDataNodeForFileResponse parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static GetDataNodeForFileResponse parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static GetDataNodeForFileResponse parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static GetDataNodeForFileResponse parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static GetDataNodeForFileResponse parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static GetDataNodeForFileResponse parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static GetDataNodeForFileResponse parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  @Override
  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(GetDataNodeForFileResponse prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  @Override
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @Override
  protected Builder newBuilderForType(
      BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code com.hepo.dfs.namenode.rpc.GetDataNodeForFileResponse}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:com.hepo.dfs.namenode.rpc.GetDataNodeForFileResponse)
      GetDataNodeForFileResponseOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return NameNodeRpcModel.internal_static_com_hepo_dfs_namenode_rpc_GetDataNodeForFileResponse_descriptor;
    }

    @Override
    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return NameNodeRpcModel.internal_static_com_hepo_dfs_namenode_rpc_GetDataNodeForFileResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              GetDataNodeForFileResponse.class, Builder.class);
    }

    // Construct using com.hepo.dfs.namenode.rpc.model.GetDataNodeForFileResponse.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
      }
    }
    @Override
    public Builder clear() {
      super.clear();
      dataNodeInfo_ = "";

      return this;
    }

    @Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return NameNodeRpcModel.internal_static_com_hepo_dfs_namenode_rpc_GetDataNodeForFileResponse_descriptor;
    }

    @Override
    public GetDataNodeForFileResponse getDefaultInstanceForType() {
      return GetDataNodeForFileResponse.getDefaultInstance();
    }

    @Override
    public GetDataNodeForFileResponse build() {
      GetDataNodeForFileResponse result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @Override
    public GetDataNodeForFileResponse buildPartial() {
      GetDataNodeForFileResponse result = new GetDataNodeForFileResponse(this);
      result.dataNodeInfo_ = dataNodeInfo_;
      onBuilt();
      return result;
    }

    @Override
    public Builder clone() {
      return super.clone();
    }
    @Override
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return super.setField(field, value);
    }
    @Override
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return super.clearField(field);
    }
    @Override
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return super.clearOneof(oneof);
    }
    @Override
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, Object value) {
      return super.setRepeatedField(field, index, value);
    }
    @Override
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return super.addRepeatedField(field, value);
    }
    @Override
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof GetDataNodeForFileResponse) {
        return mergeFrom((GetDataNodeForFileResponse)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(GetDataNodeForFileResponse other) {
      if (other == GetDataNodeForFileResponse.getDefaultInstance()) return this;
      if (!other.getDataNodeInfo().isEmpty()) {
        dataNodeInfo_ = other.dataNodeInfo_;
        onChanged();
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    @Override
    public final boolean isInitialized() {
      return true;
    }

    @Override
    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      GetDataNodeForFileResponse parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (GetDataNodeForFileResponse) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private Object dataNodeInfo_ = "";
    /**
     * <code>string dataNodeInfo = 1;</code>
     * @return The dataNodeInfo.
     */
    public String getDataNodeInfo() {
      Object ref = dataNodeInfo_;
      if (!(ref instanceof String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        dataNodeInfo_ = s;
        return s;
      } else {
        return (String) ref;
      }
    }
    /**
     * <code>string dataNodeInfo = 1;</code>
     * @return The bytes for dataNodeInfo.
     */
    public com.google.protobuf.ByteString
        getDataNodeInfoBytes() {
      Object ref = dataNodeInfo_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        dataNodeInfo_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string dataNodeInfo = 1;</code>
     * @param value The dataNodeInfo to set.
     * @return This builder for chaining.
     */
    public Builder setDataNodeInfo(
        String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      dataNodeInfo_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string dataNodeInfo = 1;</code>
     * @return This builder for chaining.
     */
    public Builder clearDataNodeInfo() {
      
      dataNodeInfo_ = getDefaultInstance().getDataNodeInfo();
      onChanged();
      return this;
    }
    /**
     * <code>string dataNodeInfo = 1;</code>
     * @param value The bytes for dataNodeInfo to set.
     * @return This builder for chaining.
     */
    public Builder setDataNodeInfoBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      dataNodeInfo_ = value;
      onChanged();
      return this;
    }
    @Override
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    @Override
    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:com.hepo.dfs.namenode.rpc.GetDataNodeForFileResponse)
  }

  // @@protoc_insertion_point(class_scope:com.hepo.dfs.namenode.rpc.GetDataNodeForFileResponse)
  private static final GetDataNodeForFileResponse DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new GetDataNodeForFileResponse();
  }

  public static GetDataNodeForFileResponse getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<GetDataNodeForFileResponse>
      PARSER = new com.google.protobuf.AbstractParser<GetDataNodeForFileResponse>() {
    @Override
    public GetDataNodeForFileResponse parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new GetDataNodeForFileResponse(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<GetDataNodeForFileResponse> parser() {
    return PARSER;
  }

  @Override
  public com.google.protobuf.Parser<GetDataNodeForFileResponse> getParserForType() {
    return PARSER;
  }

  @Override
  public GetDataNodeForFileResponse getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

