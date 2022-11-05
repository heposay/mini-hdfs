package com.hepo.dfs.backupnode.server;

import com.alibaba.fastjson.JSONArray;
import com.hepo.dfs.namenode.rpc.model.*;
import com.hepo.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

/**
 * Description:
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-06-29 14:41
 *
 * @author linhaibo
 */
public class BackupNodeRpcClient {

    private static final String NAMENODE_HOSTNAME = "localhost";

    private static final int NAMENODE_PORT = 50070;


    private static final String BACKUPNODE_HONENAME = "dfs-backup-node-01";

    private static final String BACKUPNODE_IP = "localhost";

    private static final long NAMENODE_HEARTBEAT_INTERVAL_TIME = 5 * 60 * 1000;

    private final NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;


    private Boolean isNamenodeRunning = true;


    public BackupNodeRpcClient() {
        ManagedChannel channel = NettyChannelBuilder.forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT).build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
        System.out.println("BackupNodeRpcClient已启动，Namenode的IP地址：" + NAMENODE_HOSTNAME + ", 端口号：" + NAMENODE_PORT);
    }

    public void start() {
        try {
            register();
            startHeartbeat();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 抓取日志
     *
     * @return 日志文件数组
     */
    public JSONArray fetchEditsLog(long syncedTxid) {
        FetchEditsLogRequest request = FetchEditsLogRequest.newBuilder()
                .setCode(1)
                .setSyncedTxid(syncedTxid)
                .build();
        FetchEditsLogResponse response = namenode.fetchEditsLog(request);
        String editsLogJson = response.getEditsLog();
        return JSONArray.parseArray(editsLogJson);
    }


    /**
     * 更新checkpoint txid
     *
     * @param txid 当前checkpoint之后的txid
     */
    public void updateCheckpointTxid(long txid) {
        UpdateCheckpointTxidRequest request = UpdateCheckpointTxidRequest.newBuilder()
                .setTxid(txid).build();
        UpdateCheckpointTxidResponse response = namenode.updateCheckpointTxid(request);
        System.out.println("更新checkpoint txid相应结果:" + ResponseUtil.getMsg(response.getStatus()));
    }


    public Boolean isNamenodeRunning() {
        return isNamenodeRunning;
    }

    public void setNamenodeRunning(Boolean namenodeRunning) {
        isNamenodeRunning = namenodeRunning;
    }


    /**
     * 向自己负责通信的那个NameNode进行注册
     */
    private void register() throws Exception {
        // 发送rpc接口调用请求到NameNode去进行注册
        System.out.println("发送请求到NameNode进行注册.......");
        RegisterRequest registerRequest = RegisterRequest.newBuilder().setIp(BACKUPNODE_IP).setHostname(BACKUPNODE_HONENAME).build();
        RegisterResponse response = namenode.register(registerRequest);
        System.out.println("接收到NameNode返回的注册响应：" + ResponseUtil.getMsg(response.getStatus()));
        Thread.sleep(500);
    }

    /**
     * 开启心跳线程
     */
    private void startHeartbeat() {
        new BackupNodeRpcClient.HeartbeatThread().start();
    }

    /**
     * 负责心跳的后台线程
     */
    class HeartbeatThread extends Thread {
        @Override
        public void run() {
            while (true) {
                try {
                    if (!isNamenodeRunning) {
                        System.out.println("现在NameNode服务端处于故障，不进行心跳检测");
                        Thread.sleep(3000);
                    }
                    // 通过RPC接口发送到NameNode他的注册接口上去
                    HeartbeatRequest request = HeartbeatRequest.newBuilder()
                            .setIp(BACKUPNODE_IP)
                            .setHostname(BACKUPNODE_HONENAME)
                            .build();
                    HeartbeatResponse response = namenode.heartbeat(request);
                    System.out.println("接收到NameNode返回心跳响应：" + ResponseUtil.getMsg(response.getStatus()));
                    if (ResponseStatus.SUCCESS.equals(response.getStatus())) {
                        isNamenodeRunning = true;
                    } else if (ResponseStatus.FAILURE.equals(response.getStatus())) {
                        isNamenodeRunning = true;
                        //重新注册
                        register();
                    } else {
                        isNamenodeRunning = false;
                    }

                    Thread.sleep(NAMENODE_HEARTBEAT_INTERVAL_TIME); // 每隔30秒发送一次心跳到NameNode上去
                } catch (Exception e) {
                    isNamenodeRunning = false;
                }

            }
        }
    }

}
