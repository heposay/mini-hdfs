// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: NameNodeRpcModel.proto

package com.hepo.dfs.namenode.rpc.model;

public interface InformReplicaReceivedRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:com.hepo.dfs.namenode.rpc.InformReplicaReceivedRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>string ip = 1;</code>
   * @return The ip.
   */
  String getIp();
  /**
   * <code>string ip = 1;</code>
   * @return The bytes for ip.
   */
  com.google.protobuf.ByteString
      getIpBytes();

  /**
   * <code>string hostname = 2;</code>
   * @return The hostname.
   */
  String getHostname();
  /**
   * <code>string hostname = 2;</code>
   * @return The bytes for hostname.
   */
  com.google.protobuf.ByteString
      getHostnameBytes();

  /**
   * <code>string filename = 3;</code>
   * @return The filename.
   */
  String getFilename();
  /**
   * <code>string filename = 3;</code>
   * @return The bytes for filename.
   */
  com.google.protobuf.ByteString
      getFilenameBytes();
}
