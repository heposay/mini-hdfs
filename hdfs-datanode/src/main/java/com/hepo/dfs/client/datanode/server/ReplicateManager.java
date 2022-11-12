package com.hepo.dfs.client.datanode.server;

import com.alibaba.fastjson.JSONObject;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Description: 副本复制管理组件
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/9 08:23
 *
 * @author linhaibo
 */
public class ReplicateManager {
    public static final Integer REPLICATE_THREAD_NUM = 3;

    private final FileUploadClient fileUploadClient = new FileUploadClient();

    private final ConcurrentLinkedQueue<JSONObject> replicateTaskQueue = new ConcurrentLinkedQueue<>();


    private final NameNodeRpcClient nameNodeRpcClient;

    public ReplicateManager(NameNodeRpcClient nameNodeRpcClient) {
        this.nameNodeRpcClient = nameNodeRpcClient;
        for (int i = 0; i < REPLICATE_THREAD_NUM; i++) {
            new ReplicateWorker().start();
        }
    }

    /**
     * 添加复制任务
     *
     * @param replicateTask 复制任务
     */
    public void addReplicateTask(JSONObject replicateTask) {
        replicateTaskQueue.offer(replicateTask);
    }

    /**
     * 副本复制线程
     */
    class ReplicateWorker extends Thread {
        @Override
        public void run() {
            while (true) {
                FileOutputStream fos = null;
                FileChannel fileChannel = null;
                try {
                    JSONObject replicateTask = replicateTaskQueue.poll();
                    if (replicateTask == null) {
                        Thread.sleep(1000);
                        continue;
                    }
                    System.out.println("开始执行副本复制任务,replicateTask:" + replicateTask);

                    String filename = replicateTask.getString("filename");
                    Long fileLength = replicateTask.getLong("fileLength");

                    JSONObject sourceDataNode = replicateTask.getJSONObject("sourceDataNode");
                    String hostname = sourceDataNode.getString("hostname");
                    Integer uploadServerPort = sourceDataNode.getInteger("uplaodServerPort");

                    // 跟源头数据接头通信读取图片过来
                    byte[] file = fileUploadClient.readFile(hostname, uploadServerPort, filename);
                    ByteBuffer fileBuffer = ByteBuffer.wrap(file);
                    System.out.println("从源头数据节点读取到图片，大小为：" + file.length + "字节");

                    // 根据文件的相对路径定位到绝对路径，写入本地磁盘文件中
                    String absoluteFilenamePath = FileUtils.getAbsoluteFilenamePath(filename);
                    fos = new FileOutputStream(absoluteFilenamePath);
                    fileChannel = fos.getChannel();
                    fileChannel.write(fileBuffer);
                    System.out.println("将图片写入本地磁盘文件，路径为：" + absoluteFilenamePath);

                    //进行量上报
                    nameNodeRpcClient.informReplicaReceived(filename + "_" + fileLength);
                    System.out.println("向Master节点进行增量上报......");

                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    try {
                        if (fos != null) {
                            fos.close();
                        }
                        if (fileChannel != null) {
                            fileChannel.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }


                }
            }
        }
    }


}
