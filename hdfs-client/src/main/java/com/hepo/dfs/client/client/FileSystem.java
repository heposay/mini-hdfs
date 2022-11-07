package com.hepo.dfs.client.client;

import java.io.IOException;

/**
 * 客户端文件系统的接口
 * @author zhonghuashishan
 *
 */
public interface FileSystem {

	/**
	 * 创建目录
	 * @param path 目录对应的路径
	 */
	void mkdir(String path);

	/**
	 * 优雅关闭
	 * @throws Exception
	 */
	void shutdown();

	/**
	 * 上传文件
	 * @param file 文件字节流
	 * @param filename 文件名称
	 * @param fileSize 文件大小
	 * @return boolean
	 */
	Boolean upload(byte[] file, String filename, long fileSize);


	/**
	 * 文件下载
	 * @param filename 文件名
	 * @return 文件的字节流
	 * @throws IOException
	 */
	byte[] download(String filename) throws IOException;
	
}
