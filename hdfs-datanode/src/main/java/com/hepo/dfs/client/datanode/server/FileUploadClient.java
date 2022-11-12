package com.hepo.dfs.client.datanode.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * Description: 文件上传客户端
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/4 10:41
 *
 * @author linhaibo
 */
public class FileUploadClient {

    private final static Integer READ_FILE = 2;

    /**
     * 向服务端获取文件
     *
     * @param hostname         服务端ip地址
     * @param uploadServerPort 服务端端口号
     * @param filename         文件名
     */
    public byte[] readFile(String hostname, int uploadServerPort, String filename) {
        byte[] file = null;
        SocketChannel channel = null;
        Selector selector = null;

        ByteBuffer fileLengthBuffer = null;
        ByteBuffer fileBuffer = null;
        Long fileLength = null;

        try {
            //与服务端DataNode建立短连接，发送完一个文件立刻释放网络连接
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(hostname, uploadServerPort));
            selector = Selector.open();
            channel.register(selector, SelectionKey.OP_CONNECT);

            boolean reading = true;
            while (reading) {
                //轮询selector
                selector.select();

                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();
                    //判断key的类型
                    if (key.isConnectable()) {
                        channel = (SocketChannel) key.channel();
                        if (channel.isConnectionPending()) {
                            channel.finishConnect(); //把三次握手做完，建立好TCP连接

                            //封装文件的请求数据
                            byte[] filenameBytes = filename.getBytes();

                            ByteBuffer readFileRequest = ByteBuffer.allocate(8 + filenameBytes.length);
                            readFileRequest.putInt(READ_FILE);
                            //Int对应了4个字节，放到缓冲区里去
                            readFileRequest.putInt(filenameBytes.length);
                            //把真正的文件名给放入进去
                            readFileRequest.put(filenameBytes);

                            //每次write buffer之前，一定要flip。
                            readFileRequest.flip();
                            //将缓冲区数据写到channel
                            channel.write(readFileRequest);

                            //重新监听读事件
                            channel.register(selector, SelectionKey.OP_READ);
                        }
                    } else if (key.isReadable()) {
                        //读取服务端发回来的响应
                        channel = (SocketChannel) key.channel();
                        if (fileLength == null) {
                            if (fileLengthBuffer == null) {
                                fileLengthBuffer = ByteBuffer.allocate(8);
                            }
                            channel.read(fileLengthBuffer);

                            if (!fileLengthBuffer.hasRemaining()) {
                                fileLengthBuffer.rewind();
                                fileLength = fileLengthBuffer.getLong();
                            }
                        }else {
                            if (fileBuffer == null) {
                                fileBuffer = ByteBuffer.allocate(Math.toIntExact(fileLength));
                            }
                            channel.read(fileBuffer);
                            if (!fileBuffer.hasRemaining()) {
                                fileBuffer.rewind();
                                file = fileBuffer.array();
                                reading = false;
                            }
                        }

                    }
                }
            }
            return file;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (selector != null) {
                try {
                    selector.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        }
    }

}
