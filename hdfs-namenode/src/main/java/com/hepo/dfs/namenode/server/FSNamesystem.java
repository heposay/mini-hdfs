package com.hepo.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Description: 负责管理组件的所有元数据
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-04-22 09:46
 *
 * @author linhaibo
 */
public class FSNamesystem {

    /**
     * 负责管理内存中文件目录树的组件
     */
    private FSDirectory directory;

    /**
     * 负责管理内存中edit log的组件
     */
    private FSEditLog editLog;

    /**
     * 最近一次checkpoint更新的txid
     */
    private long checkpointTxid;

    /**
     * 初始化组件
     */
    public FSNamesystem() {
        directory = new FSDirectory();
        editLog = new FSEditLog(this);
        recoverNamespace();
    }

    /**
     * 创建目录
     *
     * @param path 文件路径
     * @return 是否成功
     */
    public Boolean mkdir(String path) {
        this.directory.mkdir(path);
        this.editLog.logEdit(EditLogFactory.mkdir(path));
        return true;
    }

    /**
     * 创建文件
     *
     * @param filename 文件名，包含所在的绝对路径： /products/img001.jpg
     * @return
     */
    public Boolean create(String filename) {
        if (!directory.create(filename)) {
            return false;
        }
        //这里写一条editlog
        editLog.logEdit(EditLogFactory.create(filename));
        return true;
    }

    /**
     * 强制将缓冲区的数据刷到磁盘
     */
    public void flush() {
        this.editLog.flush();
    }

    /**
     * 获取FSEditLog组件
     *
     * @return
     */
    public FSEditLog getEditLog() {
        return editLog;
    }


    public long getCheckpointTxid() {
        return checkpointTxid;
    }

    public void setCheckpointTxid(long checkpointTxid) {
        System.out.println("接收到checkpoint txid" + checkpointTxid);
        this.checkpointTxid = checkpointTxid;
    }

    /**
     * 将checkpoint txid 保存到磁盘上去
     */
    public void saveCheckpointTxid() {
        String path = "/Users/linhaibo/Documents/tmp/editslog/checkpoint-txid.meta";

        RandomAccessFile raf = null;
        FileOutputStream out = null;
        FileChannel channel = null;

        try {
            File file = new File(path);
            if (file.exists()) {
                file.delete();
            }

            ByteBuffer dataBuffer = ByteBuffer.wrap(String.valueOf(getCheckpointTxid()).getBytes());


            raf = new RandomAccessFile(path, "rw");
            out = new FileOutputStream(raf.getFD());
            channel = out.getChannel();

            channel.write(dataBuffer);
            //强制刷盘
            channel.force(false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (channel != null) {
                    channel.close();
                }
                if (out != null) {
                    out.close();
                }
                if (raf != null) {
                    raf.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 恢复元数据
     */
    public void recoverNamespace() {
        loadFSImage();

    }

    private  void loadFSImage() {
        FileInputStream in = null;
        FileChannel channel = null;
        try {
            in = new FileInputStream("/Users/linhaibo/Documents/tmp/editslog/fsimage.meta");
            channel = in.getChannel();
            //读取数据
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
            int count = channel.read(buffer);
            buffer.flip();
            //解析数据
            String fsimageJson = new String(buffer.array(), 0, count);
            System.out.println("恢复fsimage文件中的数据：" + fsimageJson);

            FSDirectory.INode dirTree = JSONObject.parseObject(fsimageJson, FSDirectory.INode.class);
            directory.setDirTree(dirTree);

        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }
}
