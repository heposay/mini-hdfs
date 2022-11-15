package com.hepo.dfs.client.client;

/**
 * Description: 响应回调函数接口
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/16 00:34
 *
 * @author linhaibo
 */
public interface ResponseCallback {
    /**
     * 处理响应结果
     *
     * @param response 响应结果
     */
    void process(NetworkResponse response);
}
