package com.hepo.dfs.namenode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hepo.dfs.namenode.rpc.model.*;
import com.hepo.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Description: NameNode的服务接口实现类（所有的处理逻辑都在该类完成）
 * Project:  hdfs-study
 * CreateDate: Created in 2022-06-07 16:22
 *
 * @author linhaibo
 */
public class NameNodeServiceImpl extends NameNodeServiceGrpc.NameNodeServiceImplBase {


    private static final Integer STATUS_SUCCESS = 1;
    private static final Integer STATUS_FAILURE = 2;
    private static final Integer STATUS_SHUTDOWN = 3;


    /**
     * 负责管理元数据的核心组件（逻辑组件）
     */
    private FSNamesystem namesystem;
    /**
     * 负责管理集群中所有的datanode的组件
     */
    private DataNodeManager datanodeManager;

    private volatile Boolean isRunning = true;


    /**
     * 当前缓冲的一小部分editslog
     */
    private JSONArray currentBufferedEditsLog = new JSONArray();

    /**
     * 当前内存里缓冲了哪个磁盘文件的数据
     */
    private String bufferedFlushedTxid;

    /**
     * 当前内存中最大的txid
     */
    private Long currentBufferedMaxTxid = 0L;

    /**
     * 默认拉取日志的数目
     */
    private static final int BACKUP_NODE_FETCH_SIZE = 10;

    public NameNodeServiceImpl(FSNamesystem namesystem, DataNodeManager datanodeManager) {
        this.namesystem = namesystem;
        this.datanodeManager = datanodeManager;
    }

    /**
     * datanode进行注册
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
        datanodeManager.register(request.getIp(), request.getHostname());
        System.out.println("收到客户端[" + request.getIp() + StringPoolConstant.COLON + request.getHostname() + "]的注册信息");
        RegisterResponse response = RegisterResponse.newBuilder()
                .setStatus(STATUS_SUCCESS).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * 对datanode进行心跳检测
     */
    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        datanodeManager.heartbeat(request.getIp(), request.getHostname());
        System.out.println("收到客户端[" + request.getIp() + StringPoolConstant.COLON + request.getHostname() + "]的心跳信息");
        HeartbeatResponse response = HeartbeatResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();

    }

    /**
     * 创建目录（客户端）
     */
    @Override
    public void mkdir(MkdirRequest request, StreamObserver<MkdirResponse> responseObserver) {
        try {
            MkdirResponse response = null;
            if (!isRunning) {
                response = MkdirResponse.newBuilder().setStatus(STATUS_SHUTDOWN).build();
            } else {
                this.namesystem.mkdir(request.getPath());
                response = MkdirResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
                System.out.println("创建目录：path" + request.getPath());
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 优雅关闭
     */
    @Override
    public void shutdown(ShutdownRequest request, StreamObserver<ShutdownResponse> responseObserver) {
        this.isRunning = false;
        namesystem.flush();
        namesystem.saveCheckpointTxid();
        ShutdownResponse response = ShutdownResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
        System.out.println("收到客户端发来的shutdown请求:" + request.getCode());
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * 拉取editlog日志
     */
    @Override
    public void fetchEditsLog(FetchEditsLogRequest request, StreamObserver<FetchEditsLogResponse> responseObserver) {
        FetchEditsLogResponse response = null;
        //如果系统已经停止了，就不允许其他再来拉取数据
        if(!isRunning) {
            response = FetchEditsLogResponse.newBuilder()
                    .setEditsLog(new JSONArray().toJSONString())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        List<String> flushedTxids = namesystem.getEditLog().getFlushedTxids();

        JSONArray fetchedEditsLog = new JSONArray();

        //当前backupNode节点同步到了哪一条txid了
        long syncedTxid = request.getSyncedTxid();
        System.out.println("接收到的syncTxid为：" + syncedTxid);

        //如果此时还没有刷出来任何磁盘文件的话，那么此时数据仅仅存在于内存缓冲里
        if (flushedTxids.size() == 0) {
            //从内存缓冲拉数据
            System.out.println("暂时没有任何磁盘文件，直接从内存缓冲中拉取editslog......");
            fetchFromBufferedEditLog(syncedTxid, fetchedEditsLog);
        }
        //如果此时已经有磁盘文件了，这个时候就要扫描所有磁盘文件的索引范围
        else {
            if (bufferedFlushedTxid != null) {
                //如果要拉取的数据存在当前缓存的磁盘文件里
                if (existInFlushedFile(syncedTxid, bufferedFlushedTxid)) {
                    System.out.println("获取到的bufferedFlushedTxid为：" + bufferedFlushedTxid);
                    System.out.println("上一次已经缓存过磁盘文件的数据，直接从磁盘文件缓存中拉取editslog......");
                    fetchFromCurrentBuffer(syncedTxid, fetchedEditsLog);
                }
                //如果要拉取的数据不在当前缓存的磁盘文件中，需要从下一个磁盘文件去拉取
                else {

                    String nextFlushedTxid = getNextFlushedTxid(flushedTxids, bufferedFlushedTxid);
                    // 如果可以找到下一个磁盘文件，那么就从下一个磁盘文件里开始读取数据
                    if (nextFlushedTxid != null) {
                        System.out.println("上一次缓存的磁盘文件找不到要拉取的数据，从下一个磁盘文件中拉取editslog......");
                        FetchFromFlushedFile(syncedTxid, nextFlushedTxid, fetchedEditsLog);
                    } else {
                        System.out.println("上一次缓存的磁盘文件找不到要拉取的数据，而且没有下一个磁盘文件，尝试从内存缓冲中拉取editslog......");
                        // 如果没有找到下一个文件，此时就需要从内存里去继续读取
                        fetchFromBufferedEditLog(syncedTxid, fetchedEditsLog);
                    }
                }

            } else {
                //从磁盘里面读取数据
                boolean fechedFromFlushedFile = false;
                for (String flushedTxid : flushedTxids) {
                    if (existInFlushedFile(syncedTxid, flushedTxid)) {
                        System.out.println("尝试从磁盘文件中拉取editslog，flushedTxid=" + flushedTxid);
                        //此时可以把这个磁盘文件里以及下一个磁盘文件的的数据都读取出来，放到内存里来缓存
                        FetchFromFlushedFile(syncedTxid, flushedTxid, fetchedEditsLog);
                        fechedFromFlushedFile = true;
                        break;
                    }
                }
                // 第二种情况，你要拉取的txid已经比磁盘文件里的全部都新了，还在内存缓冲里
                // 如果没有找到下一个文件，此时就需要从内存里去继续读取
                if (!fechedFromFlushedFile) {
                    System.out.println("所有磁盘文件都没找到要拉取的editslog，尝试直接从内存缓冲中拉取editslog......");
                    fetchFromBufferedEditLog(syncedTxid, fetchedEditsLog);
                }
            }

        }

        response = FetchEditsLogResponse.newBuilder().setEditsLog(fetchedEditsLog.toJSONString()).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * 获取下一个磁盘文件对应的txid范围
     */
    private String getNextFlushedTxid(List<String> flushedTxids, String bufferedFlushedTxid) {
        for (int i = 0; i < flushedTxids.size(); i++) {
            if (flushedTxids.get(i).equals(bufferedFlushedTxid)) {
                if (i + 1 < flushedTxids.size()) {
                    return flushedTxids.get(i + 1);
                }
            }
        }
        return null;
    }


    /**
     * 从磁盘文件读取editLog，同时缓存到内存中
     */
    private void FetchFromFlushedFile(long syncedTxid, String flushedTxid, JSONArray fetchedEditsLog) {
        try {
            String[] flushedTxidSplited = flushedTxid.split(StringPoolConstant.UNDERLINE);
            long startTxid = Long.valueOf(flushedTxidSplited[0]);
            long endTxid = Long.valueOf(flushedTxidSplited[1]);

            //开始读取数据，把数据放在缓冲区
            String currentEditsLogPath = "/Users/linhaibo/Documents/tmp/edits-" + startTxid + StringPoolConstant.DASH + endTxid + ".log";
            //读取数据
            List<String> editLogs = Files.readAllLines(Paths.get(currentEditsLogPath));
            //清空上一次的缓存
            currentBufferedEditsLog.clear();
            for (String editLog : editLogs) {
                currentBufferedEditsLog.add(JSONObject.parseObject(editLog));
                //记录一下当前内存缓存中的最大一个txid是多少，下次过来拉取数据的时候，可以判断一下，不用每次都去内存缓冲区加载
                currentBufferedMaxTxid = JSONObject.parseObject(editLog).getLong("txid");
            }
            // 缓存了某个刷入磁盘文件的数据
            bufferedFlushedTxid = flushedTxid;

            //从当前缓冲区读取数据
            fetchFromCurrentBuffer(syncedTxid, fetchedEditsLog);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 判断flushedTxid是否存在于刷到磁盘的文件中
     */
    private Boolean existInFlushedFile(long syncedTxid, String flushedTxid) {
        String[] flushedTxidSplited = flushedTxid.split(StringPoolConstant.UNDERLINE);
        long startTxid = Long.valueOf(flushedTxidSplited[0]);
        long endTxid = Long.valueOf(flushedTxidSplited[1]);
        long fetchTxid = syncedTxid + 1;
        if (fetchTxid >= startTxid && fetchTxid <= endTxid) {
            return true;
        }
        return false;
    }


    /**
     * 从内存缓冲区拉取数据
     */
    private void fetchFromBufferedEditLog(long syncedTxid, JSONArray fetchedEditsLog) {
        //如果要拉取的txid还在上一次内存缓存中，此时继续从内存缓存中拉取即可
        long fetchTxid = syncedTxid + 1;
        if (fetchTxid <= currentBufferedMaxTxid) {
            System.out.println("尝试从内存缓冲拉取的时候，发现上一次内存缓存有数据可供拉取......");
            fetchFromCurrentBuffer(syncedTxid, fetchedEditsLog);
            return;
        } else {
            //必须重新把内存缓冲中的数据加载到内存缓存来
            currentBufferedEditsLog.clear();
            //此时数据存在内存缓冲中
            String[] bufferedEditsLog = namesystem.getEditLog().getBufferedEditsLog();
            if (bufferedEditsLog != null) {
                for (String editLog : bufferedEditsLog) {
                    currentBufferedEditsLog.add(JSONObject.parseObject(editLog));
                    //记录一下当前内存缓存中的最大一个txid是多少，下次过来拉取数据的时候，可以判断一下，不用每次都去内存缓冲区加载
                    currentBufferedMaxTxid = JSONObject.parseObject(editLog).getLong("txid");
                }

                //如果从缓冲区读取数据，则标记一下bufferedFlushedTxid为空
                bufferedFlushedTxid = null;

                fetchFromCurrentBuffer(syncedTxid, fetchedEditsLog);
            }
        }

    }

    /**
     * 从当前已经在内存里缓存的数据中拉取editslog
     */
    private void fetchFromCurrentBuffer(long syncedTxid, JSONArray fetchedEditsLog) {
        int fetchCount = 0;
        long fetchTxid = syncedTxid + 1;

        for (int i = 0; i < currentBufferedEditsLog.size(); i++) {
            if (currentBufferedEditsLog.getJSONObject(i).getLong("txid") == fetchTxid) {
                fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
                fetchTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid") + 1;
                fetchCount++;
            }
            if (fetchCount == BACKUP_NODE_FETCH_SIZE) {
                break;
            }
        }
    }

    /**
     * 更新checkpoint txid
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void updateCheckpointTxid(UpdateCheckpointTxidRequest request, StreamObserver<UpdateCheckpointTxidResponse> responseObserver) {
        long txid = request.getTxid();
        namesystem.setCheckpointTxid(txid);

        UpdateCheckpointTxidResponse response = UpdateCheckpointTxidResponse.newBuilder()
                .setStatus(STATUS_SUCCESS)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
