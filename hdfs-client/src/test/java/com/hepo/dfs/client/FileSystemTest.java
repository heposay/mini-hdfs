package com.hepo.dfs.client;

import com.hepo.dfs.client.client.FileSystem;
import com.hepo.dfs.client.client.FileSystemImpl;

/**
 * Description:
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-06-29 11:11
 *
 * @author linhaibo
 */
public class FileSystemTest {

    private static FileSystem fileSystem = new FileSystemImpl();

    public static void main(String[] args) {
        testMkdir();
        //testUpload("/image/product/iphone14pro.img");
    }


    //创建目录
    public static void testMkdir() {
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    try {
                        fileSystem.mkdir("/usr/local/redis" + j + "_" + Thread.currentThread().getName());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }

    //关闭系统
    public static void testShutdown() {
        fileSystem.shutdown();
    }

    //上传文件
    public static void testUpload(String filename) {
        fileSystem.upload(null, filename);
    }
}
