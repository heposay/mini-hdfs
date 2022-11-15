package com.hepo.dfs.backupnode.server;

/**
 * Description:
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/3 10:36
 *
 * @author linhaibo
 */
public interface ResponseStatus {

    Integer SUCCESS = 1;
    Integer FAILURE = 2;
    Integer SHUTDOWN = 3;
    Integer DULPLICATE = 4;
}
