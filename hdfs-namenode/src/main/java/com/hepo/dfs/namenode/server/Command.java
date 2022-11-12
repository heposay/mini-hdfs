package com.hepo.dfs.namenode.server;

/**
 * Description: 下发DataNode的命令
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/5 14:21
 *
 * @author linhaibo
 */
public class Command {

    public static final Integer REGISTER = 1;
    public static final Integer REPORT_COMPLETE_STORAGE_INFO = 2;

    public static final Integer REPLICATE = 3;

    public static final Integer REMOVE_REPLICATE = 4;

    private Integer type;

    private String content;

    public Command(Integer type) {
        this.type = type;
    }

    public Command() {
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
