package com.hepo.dfs.client;

import com.hepo.dfs.client.client.FileSystem;
import com.hepo.dfs.client.client.FileSystemImpl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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
        //testMkdir();
        //testShutdown();
        //testUpload("/Users/linhaibo/Documents/tmp/cat.jpeg");
        testDownload("/image/product/pig2.jpg");
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
        try {
            File file = new File(filename);
            long fileLength = file.length();

            ByteBuffer buffer = ByteBuffer.allocate((int) fileLength);

            FileInputStream fis = new FileInputStream(file);
            FileChannel channel = fis.getChannel();
            channel.read(buffer);
            buffer.flip();
            byte[] fileBytes = buffer.array();


            fileSystem.upload(fileBytes, "/image/product/pig2.jpg", fileLength);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void testDownload(String filename) {
        try {
            byte[] fileBytes = fileSystem.download(filename);
            ByteBuffer fileBuffer = ByteBuffer.wrap(fileBytes);

            FileOutputStream fos = new FileOutputStream("/Users/linhaibo/Documents/tmp/download/pig.jpg");
            FileChannel channel = fos.getChannel();
            channel.write(fileBuffer);

            fos.close();
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
