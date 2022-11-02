package com.hepo.dfs.namenode.server;

/**
 * Description: 生成EditLog的日志内容工厂
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-10-23 17:54
 *
 * @author linhaibo
 */
public class EditLogFactory {
    public static String mkdir(String path) {
        return "{'OP':'MKDIR', 'PATH':'" + path + "'}";
    }

    public static String create(String filename) {
        return "{'OP':'CREATE', 'PATH': '" + filename + "'}";
    }

}
