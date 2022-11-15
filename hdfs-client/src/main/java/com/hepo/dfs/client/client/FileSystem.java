package com.hepo.dfs.client.client;

import java.io.IOException;

/**
 * 客户端文件系统的接口
 *
 * @author zhonghuashishan
 */
public interface FileSystem {

    /**
     * 创建目录
     *
     * @param path 目录对应的路径
     */
    void mkdir(String path);

    /**
     * 优雅关闭
     *
     * @throws Exception
     */
    void shutdown();

    /**
     * 上传文件
     *
     * @param fileInfo 文件信息
     * @return boolean
     */
    Boolean upload(FileInfo fileInfo, ResponseCallback callback) throws Exception;


    /**
     * 文件下载
     *
     * @param filename 文件名
     * @return 文件的字节流
     * @throws IOException
     */
    byte[] download(String filename) throws IOException;

    /**
     * 重新上传文件
     *
     * @param fileInfo     文件信息
     * @param excludedHost 要排除的数据节点
     * @return 是否上传成功
     */
    Boolean retryUpload(FileInfo fileInfo, Host excludedHost) throws Exception;

}
