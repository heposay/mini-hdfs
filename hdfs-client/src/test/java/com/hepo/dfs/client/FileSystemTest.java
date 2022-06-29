package com.hepo.dfs.client;

import com.hepo.dfs.client.client.FileSystem;
import com.hepo.dfs.client.client.FileSystemImpl;

/**
 * Description:
 * Project:  hdfs-study
 * CreateDate: Created in 2022-06-29 11:11
 *
 * @author linhaibo
 */
public class FileSystemTest {

    public static void main(String[] args) {

        FileSystem fileSystem = new FileSystemImpl();

        for (int i = 0; i < 200; i++) {
            new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < 10; j++) {
                        try {
                            fileSystem.mkdir("/usr/local/redis" + j + "_" + Thread.currentThread().getName());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }.start();

        }

        //关闭系统
        fileSystem.shutdown();
    }
}
