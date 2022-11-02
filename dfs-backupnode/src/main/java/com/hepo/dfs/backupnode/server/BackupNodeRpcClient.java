package com.hepo.dfs.backupnode.server;

import com.alibaba.fastjson.JSONArray;
import com.hepo.dfs.namenode.rpc.model.FetchEditsLogRequest;
import com.hepo.dfs.namenode.rpc.model.FetchEditsLogResponse;
import com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest;
import com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse;
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

    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;


    public BackupNodeRpcClient() {
        ManagedChannel channel = NettyChannelBuilder.forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT).build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
        System.out.println("BackupNodeRpcClient已启动，Namenode的IP地址：" + NAMENODE_HOSTNAME + ", 端口号：" + NAMENODE_PORT);
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

}
