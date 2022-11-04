package com.hepo.dfs.namenode.server;

/**
 * Description: NameNode相关配置类
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/4 13:33
 *
 * @author linhaibo
 */
public class NameNodeConfig {
    /**
     * 心跳过期时间
     */
    public final static long HEARTBEAT_LAST_EXPIRATION_TIME = 90 * 1000;

    /**
     * 心跳检测时间间隙
     */
    public final static long HEARTBEAT_CHECK_INTERVAL_TIME = 30 * 1000;

    /**
     * DataNode副本数
     */
    public final static int DATANODE_DUPLICATE = 2;

    /**
     * NameNode的数据目录
     */
    public final static String NAMENODE_DIR = "/Users/linhaibo/Documents/tmp/editslog";

    /**
     * 单块editslog缓冲区的最大大小：默认是25kb
     */
    public static final Integer EDIT_LOG_BUFFER_LIMIT = 25 * 1024;


    /**
     * FsImagesUpload组件的默认端口
     */
    public final static int IMAGE_UPLOAD_DEFAULT_PORT = 9000;

    /**
     * FsImagesUpload组件的默认最大连接数
     */
    public final static int IMAGE_UPLOAD_DEFAULT_BACKLOG = 100;

    /**
     * 默认缓存区大小：1MB
     */
    public final static int IMAGE_UPLOAD_DEFAULT_BUFFER_SIZE = 1024 * 1024;

    /**
     * NameNode默认端口
     */
    public static final int NAMENODE_DEFAULT_PORT = 50070;

    public static final Integer STATUS_SUCCESS = 1;
    public static final Integer STATUS_FAILURE = 2;
    public static final Integer STATUS_SHUTDOWN = 3;
    public static final Integer STATUS_DUPLICATE = 4;


    /**
     * 默认拉取日志的数目
     */
    public static final int BACKUP_NODE_FETCH_SIZE = 10;
}
