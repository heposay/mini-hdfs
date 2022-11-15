package com.hepo.dfs.client.datanode.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Description: 负责执行磁盘IO的线程
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/15 13:48
 *
 * @author linhaibo
 */
public class IOThread extends Thread {
    private final static Integer REQUEST_SEND_FILE = 1;
    private final static Integer REQUEST_READ_FILE = 2;
    private NetworkRequestQueue requestQueue = NetworkRequestQueue.getInstance();

    private NameNodeRpcClient namenode;

    public IOThread(NameNodeRpcClient namenode) {
        this.namenode = namenode;
    }

    @Override
    public void run() {
        while (true) {

            try {
                NetworkRequest request = requestQueue.poll();
                if (request == null) {
                    Thread.sleep(1000);
                    continue;
                }
                Integer requestType = request.getRequestType();
                if (REQUEST_SEND_FILE.equals(requestType)) {
                    //写磁盘文件
                    writeFileToLocalDisk(request);
                } else if (REQUEST_READ_FILE.equals(requestType)) {
                    //读磁盘文件
                    readFileFromLocalDist(request);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


    /**
     * 写磁盘文件
     *
     * @param request 网络请求对象
     */
    private void writeFileToLocalDisk(NetworkRequest request) throws Exception {
        FileOutputStream fos = null;
        FileChannel fileChannel = null;
        try {
            //获取绝对路径
            String absoluteFilenamePath = request.getAbsoluteFilenamePath();
            System.out.println("准备写文件的路径:" + absoluteFilenamePath);
            fos = new FileOutputStream(absoluteFilenamePath);
            fileChannel = fos.getChannel();
            fileChannel.position(fileChannel.size());

            //往磁盘里面写数据
            int written = fileChannel.write(request.getFile());
            System.out.println("本次文件上传完毕，将" + written + " bytes的数据写入本地磁盘文件.......");

            // 增量上报Master节点自己接收到了一个文件的副本
            namenode.informReplicaReceived(request.getRelativeFilenamePath() + StringPoolConstant.UNDERLINE + request.getFileLength());
            System.out.println("增量上报收到的文件副本给NameNode节点......");

            //封装响应结果
            NetworkResponse response = new NetworkResponse();
            response.setClientAddr(request.getClientAddr());
            response.setBuffer(ByteBuffer.wrap("SUCCESS".getBytes()));

            //将响应结果放到队列中
            NetworkResponseQueue.getInstance().offer(request.getProcessorId(), response);

        } finally {
            if (fos != null) {
                fos.close();
            }
            if (fileChannel != null) {
                fileChannel.close();
            }
        }
    }

    /**
     * 读磁盘文件
     *
     * @param request 网络请求对象
     */
    private void readFileFromLocalDist(NetworkRequest request) throws Exception{
        FileInputStream fis = null;
        FileChannel fileChannel = null;
        try {
            String absoluteFilenamePath = request.getAbsoluteFilenamePath();
            System.out.println("准备读取的文件路径：" + absoluteFilenamePath);
            File file = new File(absoluteFilenamePath);
            long fileLength = file.length();

            fis = new FileInputStream(file);
            fileChannel = fis.getChannel();

            ByteBuffer fileBuffer = ByteBuffer.allocate(8 + (int) fileLength);
            fileBuffer.putLong(fileLength);
            int hasReadFileLength = fileChannel.read(fileBuffer);
            System.out.println("从本次磁盘文件中读取了" + hasReadFileLength + " bytes的数据");

            //重置buffer的position
            fileBuffer.rewind();

            //封装响应
            NetworkResponse response = new NetworkResponse();
            response.setClientAddr(request.getClientAddr());
            response.setBuffer(fileBuffer);

            //将响应结果放到响应队列中
            NetworkResponseQueue.getInstance().offer(request.getProcessorId(), response);
        }finally {
            if (fis != null) {
                fis.close();
            }
            if (fileChannel != null) {
                fileChannel.close();
            }
        }
    }
}
