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


    public static final Integer STATUS_SUCCESS = 1;
    public static final Integer STATUS_FAILURE = 2;
    public static final Integer STATUS_SHUTDOWN = 3;


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
     * 当前backupNode节点同步到了哪一条txid了
     */
    private long backupSyncTxid = 0L;

    /**
     * 当前缓冲的一小部分editslog
     */
    private JSONArray currentBufferedEditsLog = new JSONArray();

    /**
     * 当前内存里缓冲了哪个磁盘文件的数据
     */
    private String bufferedFlushedTxid;

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
        System.out.println("收到客户端["+request.getIp() + StringPoolConstant.COLON + request.getHostname() + "]的注册信息");
        RegisterResponse response = RegisterResponse.newBuilder()
                .setStatus(STATUS_SUCCESS).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * 对datanode进行心跳检测
     * @param request
     * @param responseObserver
     */
    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        datanodeManager.heartbeat(request.getIp(), request.getHostname());
        System.out.println("收到客户端["+request.getIp() + StringPoolConstant.COLON + request.getHostname() + "]的心跳信息");
        HeartbeatResponse response = HeartbeatResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();

    }

    /**
     * 创建目录（客户端）
     * @param request
     * @param responseObserver
     */
    @Override
    public void mkdir(MkdirRequest request, StreamObserver<MkdirResponse> responseObserver) {
        try {
            MkdirResponse response = null;
            if (!isRunning) {
                response = MkdirResponse.newBuilder().setStatus(STATUS_SHUTDOWN).build();
            }else {
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
        ShutdownResponse response = ShutdownResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
        System.out.println("收到客户端发来的shutdown请求:" + request.getCode());
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void fetchEditsLog(FetchEditsLogRequest request, StreamObserver<FetchEditsLogResponse> responseObserver) {
        FetchEditsLogResponse response = null;

        List<String> flushedTxids = namesystem.getEditLog().getFlushedTxids();

        JSONArray fetchedEditsLog = new JSONArray();

        if (flushedTxids.size()  == 0) {
            //获取缓冲区数据
            String[] bufferedEditsLog = namesystem.getEditLog().getBufferedEditsLog();
            if (backupSyncTxid != 0) {
                //说明已经拉取过一次了
                currentBufferedEditsLog.clear();
                for (String editLog : bufferedEditsLog) {
                    currentBufferedEditsLog.add(JSONObject.parseObject(editLog));
                }
                int fetchCount = 0;

                for (int i = 0; i < currentBufferedEditsLog.size(); i++) {
                    if (currentBufferedEditsLog.getJSONObject(i).getLong("txid") > backupSyncTxid) {
                        fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
                        //每次读取的时候，backupSyncTxid也随着更新
                        backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid");
                        fetchCount ++;
                    }
                    //满足拉取数据，则跳出循环
                    if (fetchCount == BACKUP_NODE_FETCH_SIZE) {
                        break;
                    }

                }

            }else {
                // 此时数据全部都存在于内存缓冲里
                for (String editLog : bufferedEditsLog) {
                    currentBufferedEditsLog.add(JSONObject.parseObject(editLog));
                }
                //此时可以将内存数据读取出来
                int fetchSize = Math.min(BACKUP_NODE_FETCH_SIZE, currentBufferedEditsLog.size());
                for (int i = 0; i < fetchSize; i++) {
                    fetchedEditsLog.add(currentBufferedEditsLog.get(i));
                    //每次读取的时候，backupSyncTxid也随着更新
                    backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid");
                }
            }
        }else {
            if (bufferedFlushedTxid != null) {
                //第二次进来直接读取currentBufferedEditsLog缓冲区数据
                String[] flushedTxidSplited = bufferedFlushedTxid.split(StringPoolConstant.UNDERLINE);
                long startTxid = Long.valueOf(flushedTxidSplited[0]);
                long endTxid = Long.valueOf(flushedTxidSplited[1]);
                long fetchBeginTxid = backupSyncTxid + 1;
                if (fetchBeginTxid >= startTxid && fetchBeginTxid <= endTxid) {
                    int fetchCount = 0;

                    for (int i = 0; i < currentBufferedEditsLog.size(); i++) {
                        if (currentBufferedEditsLog.getJSONObject(i).getLong("txid") > backupSyncTxid) {
                            fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
                            backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid");
                            fetchCount ++;
                        }
                        if (fetchCount == BACKUP_NODE_FETCH_SIZE) {
                            break;
                        }
                    }
                } else {
                    String nextFlushedTxid = null;
                    for (int i = 0; i < flushedTxids.size(); i++) {
                        if (flushedTxids.get(i).equals(bufferedFlushedTxid)) {
                            if (i + 1 < flushedTxids.size()) {
                                 nextFlushedTxid = flushedTxids.get(i + 1);
                            }
                        }
                    }

                    if (nextFlushedTxid != null) {
                        flushedTxidSplited = nextFlushedTxid.split(StringPoolConstant.UNDERLINE);
                        startTxid = Long.valueOf(flushedTxidSplited[0]);
                        endTxid = Long.valueOf(flushedTxidSplited[1]);
                        try {
                            currentBufferedEditsLog.clear();
                            //开始读取数据，把数据放在缓冲区
                            String currentEditsLogPath =  "/Users/linhaibo/Documents/tmp/edits-" + startTxid + StringPoolConstant.UNDERLINE + endTxid + ".log";

                            List<String> editLogs = Files.readAllLines(Paths.get(currentEditsLogPath));
                            for (String editLog : editLogs) {
                                currentBufferedEditsLog.add(JSONObject.parseObject(editLog));
                            }
                            int fetchCount = 0;

                            for (int i = 0; i < currentBufferedEditsLog.size(); i++) {
                                if (currentBufferedEditsLog.getJSONObject(i).getLong("txid") > backupSyncTxid) {
                                    fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
                                    backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid");
                                    fetchCount ++;
                                }
                                if (fetchCount == BACKUP_NODE_FETCH_SIZE) {
                                    break;
                                }
                            }

                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }else {
                        // 如果没有找到下一个文件，此时就需要从内存里去继续读取
                        bufferedFlushedTxid = null;
                        currentBufferedEditsLog.clear();

                        String[] bufferedEditsLog = namesystem.getEditLog().getBufferedEditsLog();
                        for(String editsLog : bufferedEditsLog) {
                            currentBufferedEditsLog.add(JSONObject.parseObject(editsLog));
                        }

                        int fetchCount = 0;

                        for(int i = 0; i < currentBufferedEditsLog.size(); i++) {
                            if(currentBufferedEditsLog.getJSONObject(i).getLong("txid") > backupSyncTxid) {
                                fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
                                backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid");
                                fetchCount++;
                            }
                            if(fetchCount == BACKUP_NODE_FETCH_SIZE) {
                                break;
                            }
                        }
                    }
                }

            }
            //从磁盘里面读取数据
            for (String flushedTxid : flushedTxids) {
                String[] flushedTxidSplited = flushedTxid.split(StringPoolConstant.UNDERLINE);
                long startTxid = Long.valueOf(flushedTxidSplited[0]);
                long endTxid = Long.valueOf(flushedTxidSplited[1]);
                long fetchBeginTxid = backupSyncTxid + 1;

                if (fetchBeginTxid >= startTxid && fetchBeginTxid <= endTxid) {
                    //标记一下当前已经读取了哪个磁盘文件
                    bufferedFlushedTxid = flushedTxid;
                    //清除缓冲区
                    currentBufferedEditsLog.clear();

                    try {
                        //开始读取数据
                        String currentEditsLogPath =  "/Users/linhaibo/Documents/tmp/edits-" + startTxid + StringPoolConstant.UNDERLINE + endTxid + ".log";

                        List<String> editLogs = Files.readAllLines(Paths.get(currentEditsLogPath));
                        //把数据放在缓冲区
                        for (String editLog : editLogs) {
                            currentBufferedEditsLog.add(JSONObject.parseObject(editLog));
                        }
                        int fetchCount = 0;

                        //开始往拉取缓冲区放数据
                        for (int i = 0; i < currentBufferedEditsLog.size(); i++) {
                            if (currentBufferedEditsLog.getJSONObject(i).getLong("txid") > backupSyncTxid) {
                                fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
                                backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid");
                                fetchCount ++;
                            }
                            if (fetchCount == BACKUP_NODE_FETCH_SIZE) {
                                break;
                            }
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                break;
            }
            // 第二种情况，你要拉取的txid已经比磁盘文件里的全部都新了，还在内存缓冲里
            // 如果没有找到下一个文件，此时就需要从内存里去继续读取
            currentBufferedEditsLog.clear();
            String[] bufferedEditsLog = namesystem.getEditLog().getBufferedEditsLog();
            for (String editLog : bufferedEditsLog) {
                currentBufferedEditsLog.add(JSONObject.parseObject(editLog));
            }

            int fetchCount = 0;

            for (int i = 0; i < currentBufferedEditsLog.size(); i++) {
                if (currentBufferedEditsLog.getJSONObject(i).getLong("txid") > backupSyncTxid) {
                    fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
                    backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid");
                    fetchCount ++;
                }
                if (fetchCount == BACKUP_NODE_FETCH_SIZE) {
                    break;
                }
            }


        }

        System.out.println("收到客户端发来的fetchEditsLog请求：" + request.getCode());
        response = FetchEditsLogResponse.newBuilder().setEditsLog(fetchedEditsLog.toJSONString()).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
