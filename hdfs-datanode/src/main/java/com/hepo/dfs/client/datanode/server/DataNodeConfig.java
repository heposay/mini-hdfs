package com.hepo.dfs.client.datanode.server;

/**
 * Description: DatanNode配置类
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/4 13:27
 *
 * @author linhaibo
 */
public class DataNodeConfig {

    public static final String NAMENODE_HOSTNAME = "localhost";

    public static final Integer NAMENODE_PORT = 50070;

    public static final String DATANAME_HONENAME = "localhost";

    public static final String DATANAME_IP = "192.168.1.101";

    public final static String DATA_DIR = "/Users/linhaibo/Documents/tmp/datanode1";

    public static final Integer NAMENODE_HEARTBEAT_INTERVAL_TIME = 30 * 1000;

    public static final Integer FILE_UPLOAD_SERVER_PORT = 9301;

    public static final Integer FILE_UPLOAD_SERVER_WORKER_SIZE = 3;
}
