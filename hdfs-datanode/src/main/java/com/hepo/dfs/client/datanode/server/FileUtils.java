package com.hepo.dfs.client.datanode.server;

import java.io.File;

import static com.hepo.dfs.client.datanode.server.DataNodeConfig.DATA_DIR;

/**
 * Description:
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/9 08:36
 *
 * @author linhaibo
 */
public class FileUtils {
    private static String dataDir = DATA_DIR;

    /**
     * 获取文件在本地磁盘的绝对路径
     *
     * @param relativeFilenamePath 文件相对路径
     * @return
     */
    public static String getAbsoluteFilenamePath(String relativeFilenamePath) {
        //解析文件名，然后创建目录
        String[] splitRelativeDir = relativeFilenamePath.split("/");
        StringBuilder dirPath = new StringBuilder(dataDir);
        for (int i = 1; i < splitRelativeDir.length - 1; i++) {
            dirPath.append("/").append(splitRelativeDir[i]);
        }

        File dir = new File(dirPath.toString());
        if (!dir.exists()) {
            dir.mkdirs();
        }
        //拼接最终的filename
        return dirPath + "/" + splitRelativeDir[splitRelativeDir.length - 1];
    }
}
