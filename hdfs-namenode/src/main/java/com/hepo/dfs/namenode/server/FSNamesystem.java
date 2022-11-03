package com.hepo.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

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
     * 负责管理集群里的所有的datanode的组件
     */
    private DataNodeManager dataNodeManager;

    /**
     * 最近一次checkpoint更新的txid
     */
    private long checkpointTxid;

    /**
     * 初始化组件
     */
    public FSNamesystem(DataNodeManager dataNodeManager) {
        directory = new FSDirectory();
        editLog = new FSEditLog(this);
        this.dataNodeManager = dataNodeManager;
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
        try {
            loadFSImage();
            loadCheckpointTxid();
            loadEditLog();
            loadDataInfo();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 加载fsimage文件到内存里来进行恢复
     */
    private void loadFSImage() {
        FileInputStream in = null;
        FileChannel channel = null;
        try {
            String path = "/Users/linhaibo/Documents/tmp/editslog/fsimage.meta";
            File file = new File(path);
            if (!file.exists()) {
                System.out.println("fsimage文件当前不存在，不进行恢复.......");
                return;
            }
            in = new FileInputStream(file);
            channel = in.getChannel();
            //读取数据
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
            int count = channel.read(buffer);

            //解析数据
            String fsimageJson = new String(buffer.array(), 0, count);
            System.out.println("恢复fsimage文件中的数据：" + fsimageJson);

            FSDirectory.INode dirTree = JSONObject.parseObject(fsimageJson, FSDirectory.INode.class);
            directory.setDirTree(dirTree);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
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

    /**
     * 加载checkpoint txid
     */
    private void loadCheckpointTxid() throws IOException {
        FileInputStream fis = null;
        FileChannel channel = null;
        try {
            String path = "/Users/linhaibo/Documents/tmp/editslog/checkpoint-txid.meta";
            File file = new File(path);
            if (!file.exists()) {
                System.out.println("checkpoint txid文件不存在，不进行恢复.......");
                return;
            }
            fis = new FileInputStream(file);
            channel = fis.getChannel();

            ByteBuffer buffer = ByteBuffer.allocate(1024);
            int count = channel.read(buffer);

            Long checkpointTxid = Long.valueOf(new String(buffer.array(), 0, count));
            System.out.println("恢复checkpoint txid：" + checkpointTxid);

            this.checkpointTxid = checkpointTxid;

        } finally {
            if (fis != null) {
                fis.close();
            }
            if (channel != null) {
                channel.close();
            }
        }


    }

    /**
     * 加载和回放editlog
     */
    private void loadEditLog() throws IOException {
        File dir = new File("/Users/linhaibo/Documents/tmp/editslog/");
        List<File> files = new ArrayList<>(Arrays.asList(Objects.requireNonNull(dir.listFiles())));
        files = files.stream().filter(f -> f.getName().contains("edits")).sorted((o1, o2) -> {
            Integer o1StartTxid = Integer.valueOf(o1.getName().split("-")[1]);
            Integer o2StartTxid = Integer.valueOf(o2.getName().split("-")[1]);
            return o1StartTxid - o2StartTxid;
        }).collect(Collectors.toList());

        if (files.size() == 0) {
            System.out.println("当前没有任何editlog文件，不进行恢复......");
            return;
        }

        //开始回放数据
        for (File file : files) {
            System.out.println("准备恢复editlog文件中的数据：" + file.getName());

            String[] splitName = file.getName().split(StringPoolConstant.DASH);
            long startTxid = Long.parseLong(splitName[1]);
            long endTxid = Long.parseLong(splitName[2].split("[.]")[0]);

            // 如果是checkpointTxid之后的那些editlog都要加载出来
            if (endTxid > checkpointTxid) {
                String currentEditsLogFile = "/Users/linhaibo/Documents/tmp/editslog/edits-" + startTxid + StringPoolConstant.DASH + endTxid + ".log";
                //读取editlog文件
                List<String> editLogs = Files.readAllLines(Paths.get(currentEditsLogFile), StandardCharsets.UTF_8);
                //开始回放editlog
                for (String editLogJson : editLogs) {
                    JSONObject editLog = JSONObject.parseObject(editLogJson);
                    long txid = editLog.getLongValue("txid");
                    if (txid > checkpointTxid) {
                        System.out.println("准备回放editlog" + editLogJson);
                        String op = editLog.getString("OP");
                        if ("MKDIR".equals(op)) {
                            String path = editLog.getString("PATH");
                            directory.mkdir(path);
                        } else if ("CREATE".equals(op)) {
                            String filename = editLog.getString("PATH");
                            directory.create(filename);
                        }
                    }
                }
            }
        }
    }


    /**
     * 加载datainfo信息
     */
    private void loadDataInfo() {
        FileInputStream in = null;
        FileChannel channel = null;
        try {
            String path = "/Users/linhaibo/Documents/tmp/datanode/datanode-info.meta";
            File file = new File(path);
            if (!file.exists()) {
                System.out.println("datanode-info文件当前不存在，不进行恢复.......");
                return;
            }
            in = new FileInputStream(path);
            channel = in.getChannel();
            //读取数据
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            int count = channel.read(buffer);
            buffer.flip();
            //解析数据
            String datanodeInfoJson = new String(buffer.array(), 0, count);
            System.out.println("恢复datanode info文件中的数据：" + datanodeInfoJson);

            Map<String, DataNodeInfo> datainfo = JSONObject.parseObject(datanodeInfoJson, new TypeReference<Map<String, DataNodeInfo>>() {
            });
            dataNodeManager.setDataNodeInfoMap(datainfo);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
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
