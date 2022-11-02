package com.hepo.dfs.namenode.server;

import java.util.LinkedList;
import java.util.List;

/**
 * Description:负责管理内存中文件目录树的核心组件
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-04-22 09:41
 *
 * @author linhaibo
 */
public class FSDirectory {

    /**
     * 内存中的文件目录树
     */
    private INode dirTree;

    public INode getDirTree() {
        return dirTree;
    }

    public void setDirTree(INode dirTree) {
        this.dirTree = dirTree;
    }

    /**
     * 初始化构造方法
     * 他就是一个父子层级关系的数据结构，文件目录树
     * 创建目录，删除目录，重命名目录，创建文件，删除文件，重命名文件
     * 诸如此类的一些操作，都是在维护内存里的文件目录树，其实本质都是对这个内存的数据结构进行更新
     * 先创建了一个目录层级结果：/usr/warehosue/hive
     * 如果此时来创建另外一个目录：/usr/warehouse/spark
     */
    public FSDirectory() {
        this.dirTree = new INode("/");
    }

    /**
     * 创建目录
     *
     * @param path 目录路径
     */
    public void mkdir(String path) {
        // path = /usr/warehouse/hive
        // 先判断该路径下有没有 /usr这个目录，如果没有，则创建这个目录
        // 再判断 /usr路径下有没有 warehouse有没有这个目录，没有 则创建这个目录挂在/usr目录下
        // 最终再创建hive目录挂在/usr/warehouse目录下
        synchronized (dirTree) {
            String[] paths = path.split("/");
            INode parent = dirTree;

            for (String splitPath : paths) {
                // ["","usr","warehosue","spark"]
                if ("".equals(splitPath.trim())) {
                    continue;
                }
                //parent = /   splitPath = usr
                INode dir = findDirectory(parent, splitPath);
                if (dir != null) {
                    parent = dir;
                    continue;
                }

                //如果没有找到，则创建目录，把改目录挂在parent下
                INode child = new INode(splitPath);
                parent.addChild(child);
                parent = child;
            }
        }
        //printDirTree(dirTree, "");
    }

    /**
     * 打印目录树的路径（调试使用）
     */
    @SuppressWarnings("unused")
    private void printDirTree(INode dirTree, String blank) {
        if (dirTree.getChildren().size() == 0) {
            return;
        }
        for (INode dir : dirTree.getChildren()) {
            System.out.println(blank + dir.getPath());
            printDirTree(dir, blank + " ");
        }
    }

    /**
     * 对文件目录树递归查找目录树
     *
     * @param dir  目录
     * @param path 路径
     * @return 目录
     */
    private INode findDirectory(INode dir, String path) {
        if (dir.getChildren().size() == 0) {
            return null;
        }
        for (INode child : dir.getChildren()) {
            if (child.getPath().equals(path)) {
                return child;
            }
        }
        return null;
    }

    /**
     * 创建文件
     *
     * @param filename 文件名
     * @return 是否成功
     */
    public boolean create(String filename) {
        //  /image/product/img001.jpg
        // 先把路径部分截取出来，去找对应的目录
        synchronized (dirTree) {
            String[] splitFilename = filename.split("/");
            String realFilename = splitFilename[splitFilename.length - 1];
            //遍历循环找出对应的目录
            INode parent = dirTree;
            for (int i = 1; i < splitFilename.length - 1; i++) {
                INode dir = findDirectory(dirTree, splitFilename[i]);
                if (dir != null) {
                    parent = dir;
                    continue;
                }
                INode child = new INode(splitFilename[i]);
                parent.addChild(child);
                parent = child;
            }
            //此时就获取到文件的上一级目录，查看当前目录下有没有该文件。
            if (existFile(parent, realFilename)) {
                return false;
            }

            //不存在，则创建文件
            INode file = new INode(realFilename);
            parent.addChild(file);
            return true;
        }

    }

    /**
     * 当前目录下是否存在该文件
     *
     * @param dir      目录
     * @param filename 文件名
     * @return 是否存在文件
     */
    public boolean existFile(INode dir, String filename) {
        if (dir.getChildren() != null && dir.getChildren().size() > 0) {
            for (INode child : dir.getChildren()) {
                if (child.getPath().equals(filename)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 代表文件目录树中的一个节点
     */
    public static class INode {
        private String path;
        private List<INode> children;

        public INode() {
        }

        public INode(String path) {
            this.path = path;
            this.children = new LinkedList<>();
        }

        public void addChild(INode child) {
            this.children.add(child);
        }


        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public List<INode> getChildren() {
            return children;
        }

        public void setChildren(List<INode> children) {
            this.children = children;
        }

        @Override
        public String toString() {
            return "INode{" +
                    "path='" + path + '\'' +
                    ", children=" + children +
                    '}';
        }
    }

}