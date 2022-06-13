package com.hepo.dfs.namenode.server;

import java.util.LinkedList;
import java.util.List;

/**
 * Description:负责管理内存中文件目录树的核心组件
 * Project:  hdfs_study
 * CreateDate: Created in 2022-04-22 09:41
 *
 * @author linhaibo
 */
public class FSDirectory {

    /**
     * 内存中的文件目录树
     */
    private INodeDirectory dirTree;


    /**
     * 初始化构造方法
     *  他就是一个父子层级关系的数据结构，文件目录树
     *  创建目录，删除目录，重命名目录，创建文件，删除文件，重命名文件
     *  诸如此类的一些操作，都是在维护内存里的文件目录树，其实本质都是对这个内存的数据结构进行更新
     *  先创建了一个目录层级结果：/usr/warehosue/hive
     *  如果此时来创建另外一个目录：/usr/warehouse/spark
     */
    public FSDirectory() {
        this.dirTree = new INodeDirectory("/");
    }

    /**
     * 创建目录
     *
     * @param path 目录路径
     * @return
     */
    public void mkdir(String path) {
        //  path = /usr/warehouse/hive
        // 先判断该路径下有没有 /usr这个目录，如果没有，则创建这个目录
        // 再判断 /usr路径下有没有 warehouse有没有这个目录，没有 则创建这个目录挂在/usr目录下
        // 最终再创建hive目录挂在/usr/warehouse目录下
        synchronized (dirTree) {
            String[] paths = path.split("/");
            INodeDirectory parent = dirTree;

            for (String splitPath : paths) {
                // ["","usr","warehosue","spark"]
                if ("".equals(splitPath.trim())) {
                    continue;
                }
                //parent = /   splitPath = usr
                INodeDirectory dir = findDirectory(parent, splitPath);
                if (dir != null) {
                    parent = dir;
                    continue;
                }

                //如果没有找到，则创建目录，把改目录挂在parent下
                INodeDirectory child = new INodeDirectory(splitPath);
                parent.addChild(child);
                parent = child;
            }
        }
        printDirTree(dirTree, "");
    }

    /**
     * 打印目录树的路径
     * @param dirTree
     * @param blank
     */
    private void printDirTree(INodeDirectory dirTree, String blank) {
        if(dirTree.getChild().size() == 0) {
            return;
        }
        for(INode dir : dirTree.getChild()) {
            System.out.println(blank + ((INodeDirectory) dir).getPath());
            printDirTree((INodeDirectory) dir, blank + " ");
        }
    }

    /**
     * 对文件目录树递归查找目录树
     *
     * @param dir  目录
     * @param path 路径
     * @return
     */
    private INodeDirectory findDirectory(INodeDirectory dir, String path) {
        if (dir.getChild().size() == 0) {
            return null;
        }
        for (INode child : dir.getChild()) {
            if (child instanceof INodeDirectory) {
                INodeDirectory childDir = (INodeDirectory) child;
                if (childDir.getPath().equals(path)) {
                    return childDir;
                }
            }
        }
        return null;
    }

    /**
     * 代表文件目录树中的一个节点
     */
    private interface INode {

    }

    /**
     * 代表文件目录树中的一个目录
     */
    private class INodeDirectory implements INode {
        private String path;

        private List<INode> child;

        public INodeDirectory(String path) {
            this.path = path;
            this.child = new LinkedList<INode>();
        }

        public void addChild(INode node) {
            this.child.add(node);
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public List<INode> getChild() {
            return child;
        }

        public void setChild(List<INode> child) {
            this.child = child;
        }

        @Override
        public String toString() {
            return "INodeDirectory{" +
                    "path='" + path + '\'' +
                    ", child=" + child +
                    '}';
        }
    }

    /**
     * 代表文件目录树中的一个文件
     */
    private class INodeFile implements INode {

        private String name;

        public INodeFile(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
