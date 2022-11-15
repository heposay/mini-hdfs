package com.hepo.dfs.client;

import com.hepo.dfs.client.client.FileInfo;
import com.hepo.dfs.client.client.FileSystem;
import com.hepo.dfs.client.client.FileSystemImpl;
import com.hepo.dfs.client.client.Host;

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
    public static void testUpload(String filename) throws Exception{
        File file = new File(filename);
        long fileLength = file.length();

        ByteBuffer buffer = ByteBuffer.allocate((int) fileLength);

        FileInputStream fis = new FileInputStream(file);
        FileChannel channel = fis.getChannel();
        channel.read(buffer);
        buffer.flip();
        byte[] fileBytes = buffer.array();

        FileInfo fileInfo = new FileInfo();
        fileInfo.setFile(fileBytes);
        fileInfo.setFileLength(fileLength);
        fileInfo.setFileName(filename);
        fileSystem.upload(fileInfo, response -> {
            if (response.isError()) {
                Host excludedHost = new Host();
                excludedHost.setHostname(response.getHostname());
                excludedHost.setIp(response.getIp());
                try {
                    fileSystem.retryUpload(fileInfo, excludedHost);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }else {
                ByteBuffer buffer1 = response.getBuffer();
                String responseContent = new String(buffer1.array(), 0, buffer1.remaining());
                System.out.println("文件上传完毕，响应结果为：" + responseContent);
            }
        });
        System.out.println("已异步上传文件，等待结果");
        fis.close();
        channel.close();
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
