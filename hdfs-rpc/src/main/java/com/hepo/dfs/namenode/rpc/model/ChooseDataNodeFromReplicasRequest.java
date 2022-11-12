// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: NameNodeRpcModel.proto

package com.hepo.dfs.namenode.rpc.model;

/**
 * Protobuf type {@code com.hepo.dfs.namenode.rpc.ChooseDataNodeFromReplicasRequest}
 */
public  final class ChooseDataNodeFromReplicasRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:com.hepo.dfs.namenode.rpc.ChooseDataNodeFromReplicasRequest)
    ChooseDataNodeFromReplicasRequestOrBuilder {
private static final long serialVersionUID = 0L;
  // Use ChooseDataNodeFromReplicasRequest.newBuilder() to construct.
  private ChooseDataNodeFromReplicasRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private ChooseDataNodeFromReplicasRequest() {
    filename_ = "";
    excludedDataNodeId_ = "";
  }

  @Override
  @SuppressWarnings({"unused"})
  protected Object newInstance(
      UnusedPrivateParameter unused) {
    return new ChooseDataNodeFromReplicasRequest();
  }

  @Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private ChooseDataNodeFromReplicasRequest(
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

            filename_ = s;
            break;
          }
          case 18: {
            String s = input.readStringRequireUtf8();

            excludedDataNodeId_ = s;
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
    return NameNodeRpcModel.internal_static_com_hepo_dfs_namenode_rpc_ChooseDataNodeFromReplicasRequest_descriptor;
  }

  @Override
  protected FieldAccessorTable
      internalGetFieldAccessorTable() {
    return NameNodeRpcModel.internal_static_com_hepo_dfs_namenode_rpc_ChooseDataNodeFromReplicasRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            ChooseDataNodeFromReplicasRequest.class, Builder.class);
  }

  public static final int FILENAME_FIELD_NUMBER = 1;
  private volatile Object filename_;
  /**
   * <code>string filename = 1;</code>
   * @return The filename.
   */
  public String getFilename() {
    Object ref = filename_;
    if (ref instanceof String) {
      return (String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      String s = bs.toStringUtf8();
      filename_ = s;
      return s;
    }
  }
  /**
   * <code>string filename = 1;</code>
   * @return The bytes for filename.
   */
  public com.google.protobuf.ByteString
      getFilenameBytes() {
    Object ref = filename_;
    if (ref instanceof String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (String) ref);
      filename_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int EXCLUDEDDATANODEID_FIELD_NUMBER = 2;
  private volatile Object excludedDataNodeId_;
  /**
   * <code>string excludedDataNodeId = 2;</code>
   * @return The excludedDataNodeId.
   */
  public String getExcludedDataNodeId() {
    Object ref = excludedDataNodeId_;
    if (ref instanceof String) {
      return (String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      String s = bs.toStringUtf8();
      excludedDataNodeId_ = s;
      return s;
    }
  }
  /**
   * <code>string excludedDataNodeId = 2;</code>
   * @return The bytes for excludedDataNodeId.
   */
  public com.google.protobuf.ByteString
      getExcludedDataNodeIdBytes() {
    Object ref = excludedDataNodeId_;
    if (ref instanceof String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (String) ref);
      excludedDataNodeId_ = b;
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
    if (!getFilenameBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, filename_);
    }
    if (!getExcludedDataNodeIdBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, excludedDataNodeId_);
    }
    unknownFields.writeTo(output);
  }

  @Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (!getFilenameBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, filename_);
    }
    if (!getExcludedDataNodeIdBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, excludedDataNodeId_);
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
    if (!(obj instanceof ChooseDataNodeFromReplicasRequest)) {
      return super.equals(obj);
    }
    ChooseDataNodeFromReplicasRequest other = (ChooseDataNodeFromReplicasRequest) obj;

    if (!getFilename()
        .equals(other.getFilename())) return false;
    if (!getExcludedDataNodeId()
        .equals(other.getExcludedDataNodeId())) return false;
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
    hash = (37 * hash) + FILENAME_FIELD_NUMBER;
    hash = (53 * hash) + getFilename().hashCode();
    hash = (37 * hash) + EXCLUDEDDATANODEID_FIELD_NUMBER;
    hash = (53 * hash) + getExcludedDataNodeId().hashCode();
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static ChooseDataNodeFromReplicasRequest parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static ChooseDataNodeFromReplicasRequest parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static ChooseDataNodeFromReplicasRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static ChooseDataNodeFromReplicasRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static ChooseDataNodeFromReplicasRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static ChooseDataNodeFromReplicasRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static ChooseDataNodeFromReplicasRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static ChooseDataNodeFromReplicasRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static ChooseDataNodeFromReplicasRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static ChooseDataNodeFromReplicasRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static ChooseDataNodeFromReplicasRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static ChooseDataNodeFromReplicasRequest parseFrom(
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
  public static Builder newBuilder(ChooseDataNodeFromReplicasRequest prototype) {
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
   * Protobuf type {@code com.hepo.dfs.namenode.rpc.ChooseDataNodeFromReplicasRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:com.hepo.dfs.namenode.rpc.ChooseDataNodeFromReplicasRequest)
      ChooseDataNodeFromReplicasRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return NameNodeRpcModel.internal_static_com_hepo_dfs_namenode_rpc_ChooseDataNodeFromReplicasRequest_descriptor;
    }

    @Override
    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return NameNodeRpcModel.internal_static_com_hepo_dfs_namenode_rpc_ChooseDataNodeFromReplicasRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              ChooseDataNodeFromReplicasRequest.class, Builder.class);
    }

    // Construct using com.hepo.dfs.namenode.rpc.model.ChooseDataNodeFromReplicasRequest.newBuilder()
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
      filename_ = "";

      excludedDataNodeId_ = "";

      return this;
    }

    @Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return NameNodeRpcModel.internal_static_com_hepo_dfs_namenode_rpc_ChooseDataNodeFromReplicasRequest_descriptor;
    }

    @Override
    public ChooseDataNodeFromReplicasRequest getDefaultInstanceForType() {
      return ChooseDataNodeFromReplicasRequest.getDefaultInstance();
    }

    @Override
    public ChooseDataNodeFromReplicasRequest build() {
      ChooseDataNodeFromReplicasRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @Override
    public ChooseDataNodeFromReplicasRequest buildPartial() {
      ChooseDataNodeFromReplicasRequest result = new ChooseDataNodeFromReplicasRequest(this);
      result.filename_ = filename_;
      result.excludedDataNodeId_ = excludedDataNodeId_;
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
      if (other instanceof ChooseDataNodeFromReplicasRequest) {
        return mergeFrom((ChooseDataNodeFromReplicasRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(ChooseDataNodeFromReplicasRequest other) {
      if (other == ChooseDataNodeFromReplicasRequest.getDefaultInstance()) return this;
      if (!other.getFilename().isEmpty()) {
        filename_ = other.filename_;
        onChanged();
      }
      if (!other.getExcludedDataNodeId().isEmpty()) {
        excludedDataNodeId_ = other.excludedDataNodeId_;
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
      ChooseDataNodeFromReplicasRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (ChooseDataNodeFromReplicasRequest) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private Object filename_ = "";
    /**
     * <code>string filename = 1;</code>
     * @return The filename.
     */
    public String getFilename() {
      Object ref = filename_;
      if (!(ref instanceof String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        filename_ = s;
        return s;
      } else {
        return (String) ref;
      }
    }
    /**
     * <code>string filename = 1;</code>
     * @return The bytes for filename.
     */
    public com.google.protobuf.ByteString
        getFilenameBytes() {
      Object ref = filename_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        filename_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string filename = 1;</code>
     * @param value The filename to set.
     * @return This builder for chaining.
     */
    public Builder setFilename(
        String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      filename_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string filename = 1;</code>
     * @return This builder for chaining.
     */
    public Builder clearFilename() {
      
      filename_ = getDefaultInstance().getFilename();
      onChanged();
      return this;
    }
    /**
     * <code>string filename = 1;</code>
     * @param value The bytes for filename to set.
     * @return This builder for chaining.
     */
    public Builder setFilenameBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      filename_ = value;
      onChanged();
      return this;
    }

    private Object excludedDataNodeId_ = "";
    /**
     * <code>string excludedDataNodeId = 2;</code>
     * @return The excludedDataNodeId.
     */
    public String getExcludedDataNodeId() {
      Object ref = excludedDataNodeId_;
      if (!(ref instanceof String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        excludedDataNodeId_ = s;
        return s;
      } else {
        return (String) ref;
      }
    }
    /**
     * <code>string excludedDataNodeId = 2;</code>
     * @return The bytes for excludedDataNodeId.
     */
    public com.google.protobuf.ByteString
        getExcludedDataNodeIdBytes() {
      Object ref = excludedDataNodeId_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        excludedDataNodeId_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string excludedDataNodeId = 2;</code>
     * @param value The excludedDataNodeId to set.
     * @return This builder for chaining.
     */
    public Builder setExcludedDataNodeId(
        String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      excludedDataNodeId_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string excludedDataNodeId = 2;</code>
     * @return This builder for chaining.
     */
    public Builder clearExcludedDataNodeId() {
      
      excludedDataNodeId_ = getDefaultInstance().getExcludedDataNodeId();
      onChanged();
      return this;
    }
    /**
     * <code>string excludedDataNodeId = 2;</code>
     * @param value The bytes for excludedDataNodeId to set.
     * @return This builder for chaining.
     */
    public Builder setExcludedDataNodeIdBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      excludedDataNodeId_ = value;
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


    // @@protoc_insertion_point(builder_scope:com.hepo.dfs.namenode.rpc.ChooseDataNodeFromReplicasRequest)
  }

  // @@protoc_insertion_point(class_scope:com.hepo.dfs.namenode.rpc.ChooseDataNodeFromReplicasRequest)
  private static final ChooseDataNodeFromReplicasRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new ChooseDataNodeFromReplicasRequest();
  }

  public static ChooseDataNodeFromReplicasRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<ChooseDataNodeFromReplicasRequest>
      PARSER = new com.google.protobuf.AbstractParser<ChooseDataNodeFromReplicasRequest>() {
    @Override
    public ChooseDataNodeFromReplicasRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new ChooseDataNodeFromReplicasRequest(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<ChooseDataNodeFromReplicasRequest> parser() {
    return PARSER;
  }

  @Override
  public com.google.protobuf.Parser<ChooseDataNodeFromReplicasRequest> getParserForType() {
    return PARSER;
  }

  @Override
  public ChooseDataNodeFromReplicasRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

