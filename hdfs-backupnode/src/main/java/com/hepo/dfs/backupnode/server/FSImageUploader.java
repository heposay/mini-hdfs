package com.hepo.dfs.backupnode.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * Description: 负责上传fsimage到NameNode线程
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-07-13 10:44
 *
 * @author linhaibo
 */
public class FSImageUploader extends Thread {


    private final FSImage fsimage;

    private static final String DEFAULT_HOSTNAME = "localhost";

    private static final int DEFAULT_PORT = 9000;


    public FSImageUploader(FSImage fsimage) {
        this.fsimage = fsimage;
    }

    @Override
    public void run() {
        SocketChannel channel = null;
        Selector selector = null;
        try {
            //与NameNode服务端建立连接
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(DEFAULT_HOSTNAME, DEFAULT_PORT));

            selector = Selector.open();
            channel.register(selector, SelectionKey.OP_CONNECT);

            boolean uploading = true;

            while (uploading) {
                selector.select();

                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();
                while (keysIterator.hasNext()) {
                    SelectionKey key = keysIterator.next();
                    keysIterator.remove();

                    if (key.isConnectable()) {
                        channel = (SocketChannel) key.channel();

                        if (channel.isConnectionPending()) {
                            //建立好连接之后，马上上传fsimage文件到NameNode服务器
                            channel.finishConnect();
                            ByteBuffer buffer = ByteBuffer.wrap(fsimage.getFSImageJson().getBytes());
                            System.out.println("准备上传fsimage文件数据，大小为：" + buffer.capacity());
                            channel.write(buffer);
                        }
                        //注册读事件
                        channel.register(selector, SelectionKey.OP_READ);
                    } else if (key.isReadable()) {
                        ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
                        channel = (SocketChannel) key.channel();
                        int count = channel.read(buffer);
                        //收到响应信息，打印日志
                        if (count > 0) {
                            System.out.println("上传fsimage文件成功，响应消息为：" +
                                    new String(buffer.array(), 0, count));
                            channel.close();
                            //将标志位置为false，退出循环
                            uploading = false;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //关闭流
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (selector != null) {
                try {
                    selector.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
