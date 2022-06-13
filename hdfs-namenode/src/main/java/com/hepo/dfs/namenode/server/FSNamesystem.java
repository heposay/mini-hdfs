package com.hepo.dfs.namenode.server;

/**
 * Description: 负责管理组件的所有元数据
 * Project:  hdfs_study
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
     * 初始化组件
     */
    public FSNamesystem() {
        directory = new FSDirectory();
        editLog = new FSEditLog();
    }

    public Boolean mkdir (String path) throws Exception{
        this.directory.mkdir(path);
        this.editLog.logEdit( "创建了一个目录：" + path);
        return true;
    }
}
