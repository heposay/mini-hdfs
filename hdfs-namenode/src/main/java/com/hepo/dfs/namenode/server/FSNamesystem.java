package com.hepo.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.hepo.dfs.namenode.server.NameNodeConfig.NAMENODE_DIR;

/**
 * Description: 负责管理组件的所有元数据
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-04-22 09:46
 *
 * @author linhaibo
 */
public class FSNamesystem {

    /**
     * 副本数量
     */
    public static final Integer REPLICA_NUM = 2;

    /**
     * 负责管理内存中文件目录树的组件
     */
    private final FSDirectory directory;

    /**
     * 负责管理内存中edit log的组件
     */
    private final FSEditLog editLog;

    /**
     * 负责管理集群里的所有的datanode的组件
     */
    private final DataNodeManager dataNodeManager;

    /**
     * 最近一次checkpoint更新的txid
     */
    private long checkpointTxid;

    /**
     * 每个文件对应的副本所在的DataNode
     */
    private final Map<String, List<DataNodeInfo>> replicasByFilename = new HashMap<>();

    /**
     * 每个DataNode对应的所有的文件副本
     */
    private final Map<String, List<String>> filesByDataNode = new HashMap<>();

    /**
     * 文件副本持有的读写锁
     */
    private final ReentrantReadWriteLock replicasByFilenameLock = new ReentrantReadWriteLock();

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
        String path = NAMENODE_DIR + "/checkpoint-txid.meta";

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
            String path = NAMENODE_DIR + "/fsimage.meta";
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
            String path = NAMENODE_DIR + "/checkpoint-txid.meta";
            File file = new File(path);
            if (!file.exists()) {
                System.out.println("checkpoint txid文件不存在，不进行恢复.......");
                return;
            }
            fis = new FileInputStream(file);
            channel = fis.getChannel();

            ByteBuffer buffer = ByteBuffer.allocate(1024);
            int count = channel.read(buffer);

            long checkpointTxid = Long.parseLong(new String(buffer.array(), 0, count));
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
        File dir = new File(NAMENODE_DIR + "/");
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
        //fixbug:更新缓冲区txid
        editLog.setTxidSeq(Long.parseLong(files.get(files.size() - 1).getName().split(StringPoolConstant.DASH)[2].split("[.]")[0]));

        //开始回放数据
        for (File file : files) {
            System.out.println("准备恢复editlog文件中的数据：" + file.getName());

            String[] splitName = file.getName().split(StringPoolConstant.DASH);
            long startTxid = Long.parseLong(splitName[1]);
            long endTxid = Long.parseLong(splitName[2].split("[.]")[0]);

            // 如果是checkpointTxid之后的那些editlog都要加载出来
            if (endTxid > checkpointTxid) {
                String currentEditsLogFile = NAMENODE_DIR + "/edits-" + startTxid + StringPoolConstant.DASH + endTxid + ".log";
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
     * todo 修改目录名
     *
     * @param path 目录路径
     * @return 是否成功
     */
    public Boolean rename(String path) {
        return true;
    }

    /**
     * todo 删除目录
     *
     * @param path 目录路径
     * @return 是否成功
     */
    public Boolean delete(String path) {
        return true;
    }

    /**
     * 给指定的文件增加一个成功接收的文件副本
     *
     * @param ip       ip
     * @param hostname 主机名
     * @param filename 文件名
     */
    public void addReceivedReplica(String ip, String hostname, String filename, Long fileLength) {
        try {
            replicasByFilenameLock.writeLock().lock();
            //维护每个副本所在的数据节点
            List<DataNodeInfo> replicas = replicasByFilename.computeIfAbsent(filename, k -> new ArrayList<>());

            DataNodeInfo datanode = dataNodeManager.getDataNodeInfo(ip, hostname);
            replicas.add(datanode);

            //检查当前文件的副本数量是否超标
            if (replicas.size() == REPLICA_NUM) {
                //减少这个节点的存储数据量
                datanode.addStoredDataSize(-fileLength);

                //生成副本复制任务
                RemoveReplicateTask removeReplicateTask = new RemoveReplicateTask(filename, datanode);
                datanode.addRemoveReplicateTask(removeReplicateTask);
                return;
            }
            // 如果副本数量未超标，才会将副本放入数据结构中
            replicas.add(datanode);

            //维护每个数据节点的所有副本
            List<String> files = filesByDataNode.computeIfAbsent(datanode.getId(), k -> new ArrayList<>());
            files.add(filename + StringPoolConstant.UNDERLINE + fileLength);
            System.out.println("收到增量上报，当前的副本信息为：" + replicasByFilename + "," + filesByDataNode);
        } finally {
            replicasByFilenameLock.writeLock().unlock();
        }
    }

    /**
     * 删除数据节点的文件副本的数据结构
     */
    public void removeDeadDataNode(DataNodeInfo deadDataNode) {
        try {
            replicasByFilenameLock.writeLock().lock();
            List<String> filenames = filesByDataNode.get(deadDataNode.getId());
            for (String filename : filenames) {
                List<DataNodeInfo> replicas = replicasByFilename.get(filename.split(StringPoolConstant.UNDERLINE)[0]);
                replicas.remove(deadDataNode);
            }
            filesByDataNode.remove(deadDataNode.getId());
            System.out.println("从内存数据结构中删除掉这个数据节点关联的数据，" + replicasByFilename + "，" + filesByDataNode);
        } finally {
            replicasByFilenameLock.writeLock().unlock();
        }
    }

    /**
     * 获取数据节点包含的文件
     *
     * @param ip       DataNode IP地址
     * @param hostname Datanode 主机名
     * @return 文件副本集合
     */
    public List<String> getFilesByDataNode(String ip, String hostname) {
        try {
            replicasByFilenameLock.readLock().lock();
            System.out.println("当前filesByDatanode为" + filesByDataNode + "，将要以key=" + ip + "-" + hostname + "获取文件列表");
            return filesByDataNode.get(ip + StringPoolConstant.DASH + hostname);
        } finally {
            replicasByFilenameLock.readLock().unlock();
        }
    }

    /**
     * 获取复制任务的源头数据节点
     *
     * @param filename     文件名
     * @param deadDataNode 宕机的Datanode节点
     * @return 源头数据节点
     */
    public DataNodeInfo getReplicateSource(String filename, DataNodeInfo deadDataNode) {
        DataNodeInfo replicateSource = null;
        try {
            replicasByFilenameLock.readLock().lock();
            List<DataNodeInfo> replicas = replicasByFilename.get(filename);
            for (DataNodeInfo replica : replicas) {
                if (!replica.equals(deadDataNode)) {
                    replicateSource = replica;
                }
            }
        } finally {
            replicasByFilenameLock.readLock().unlock();
        }
        return replicateSource;
    }

    /**
     * 获取文件副本所在的DataNode节点信息
     *
     * @param filename
     * @return
     */
    public DataNodeInfo chooseDataNodeFromReplicas(String filename, String excludedDataNodeId) {
        try {
            replicasByFilenameLock.readLock().lock();
            DataNodeInfo excludedDataNode = dataNodeManager.getDataNodeInfo(excludedDataNodeId);
            List<DataNodeInfo> dataNodeInfos = replicasByFilename.get(filename);
            int size = dataNodeInfos.size();
            if (size == 1) {
                if (dataNodeInfos.get(0).equals(excludedDataNode)) {
                    return null;
                }
            }
            Random random = new Random();
            while (true) {
                int index = random.nextInt(size);
                DataNodeInfo dataNodeInfo = dataNodeInfos.get(index);
                if (!dataNodeInfo.equals(excludedDataNode)) {
                    return dataNodeInfo;
                }
            }
        } finally {
            replicasByFilenameLock.readLock().unlock();
        }
    }

    /**
     * 从数据节点删除掉一个文件副本
     *
     * @param id   dataNode标识
     * @param file 文件
     */
    public void removeReplicaFromDataNode(String id, String file) {
        try {
            replicasByFilenameLock.writeLock().lock();

            filesByDataNode.get(id).remove(file);
            Iterator<DataNodeInfo> iterator = replicasByFilename.get(file.split("_")[0]).iterator();
            while (iterator.hasNext()) {
                DataNodeInfo dataNodeInfo = iterator.next();
                if (dataNodeInfo.getId().equals(id)) {
                    iterator.remove();
                }

            }
        } finally {
            replicasByFilenameLock.writeLock().unlock();
        }
    }
}
