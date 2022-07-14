package com.hepo.dfs.backupnode.server;

import com.alibaba.fastjson.JSONArray;
import com.hepo.dfs.namenode.rpc.model.FetchEditsLogRequest;
import com.hepo.dfs.namenode.rpc.model.FetchEditsLogResponse;
import com.hepo.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest;
import com.hepo.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

/**
 * Description:
 * Project:  hdfs-study
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
    }


    /**
     * 抓取日志
     * @return
     */
    public JSONArray fetchEditsLog(long syncedTxid) {
        FetchEditsLogRequest request = FetchEditsLogRequest.newBuilder()
                .setCode(1)
                .setSyncedTxid(syncedTxid)
                .build();
        FetchEditsLogResponse response = namenode.fetchEditsLog(request);
        String editsLogJson = response.getEditsLog();
        System.out.println("backupNode向NameNode拉取editsLog结果：" + editsLogJson);
        return JSONArray.parseArray(editsLogJson);
    }

    /**
     * 更新checkpoint txid
     * @param txid
     */
    public void updateCheckpointTxid(long txid) {
        UpdateCheckpointTxidRequest request = UpdateCheckpointTxidRequest.newBuilder()
                .setTxid(txid).build();
        namenode.updateCheckpointTxid(request);
    }

}
