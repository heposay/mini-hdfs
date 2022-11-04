# mini-hdfs

## 历史版本

* 0.0.1 - 20220512
  *  初始化提交
  

## 项目介绍
 基于底层NIO，Netty的项目实战，来实现一个分布式存储中间件，含金量极大希望能一直更新下去



## 环境要求

Window/MacOS/Linux都可以运行

JDK8 以上

Maven 3.6.0

libprotoc 3.19.4 (https://github.com/protocolbuffers/protobuf/releases找到对应的版本)



## 重要组件介绍

* NameNode   整个Hdfs系统的元数据管理中心，里面管理着文件目录树，DataNode的节点信息，EditLog文件的生成等，负责转发客户端的请求，将数据传输给DataNode

* BackupNode：NameNode的备份节点，主要用于文件目录树的同步，FSimage文件快照的生成并存储，也承担着高可用的角色，主节点挂了，BackupNode节点可以顶替主节点继续工作

* DataNode：真正处理数据的组件，将接收到的文件存储到磁盘中。DataNode要注册到NameNode中，方便集中管理，并且定时向NameNode上传心跳检测信息，保证节点故障感知并转移

* Hdfs-Client：与NameNode互动的客户端工具类，里面封装了核心的上传文件、创建文件、关闭服务端等方法

* Hdfs-RPC：存放着Hdfs系统的所有RPC代码，这些代码都是由grpc-gen组件来自动生成

* grpc-gen：用于生成Hdfs系统的RPC代码的组件，主要编辑 /proto目录下的NameNodeRpcModel.proto， NameNodeRpcServer.proto等文件，执行mvn clean package等命令，即可生成RPC代码

  

## 项目启动流程

1. 先启动NameNode组件，它作为元数据的管理中心，所以必须放在第一位启动，可以自定义配置监听的端口。目前FsImagesUpload组件的默认端口 9000，这个组件是用于跟BackupNode同步使用的端口，默认不用改。 NameNodeRpcServer组件的默认端口50070，这个组件是客户端发送请求的时候就是监听该端口，这个可以由用户自定义配置。
2. 启动DataNode组件，它作为数据真正存储到磁盘的组件，也是非常核心的组件之一。
3. 启动BackupNode组件，该组件作为NameNode的高可用组件，也是很核心的
4. 通过Hdfs-Client调用相关的接口，既可以跟NameNode进行交互使用。



## 架构图

![image-20221102111806414](/Users/linhaibo/Documents/code/personal/mini-hdfs/README.assets/架构图.png)





## 项目细节

1.引入editslog，即使NameNode宕机了，也不会造成内存中的所有数据全部丢失。

在/usr目录下创建了一个文件，access.log，edits log。对于Hdfs系统来说，只是一条操作日志。

如果此时namenode宕机了，会怎么样？不要紧的，磁盘文件里存有一份edtislog的数据，此时重启namenode，就可以读取全部的editslog来回放日志，重新把日志对应的操作在内存文件目录树上执行一遍，此时就可以恢复出来一份完整的数据

因为edits log会先在内存缓冲里等待一会儿，所以说此时，如果 有些edits log还没刷入磁盘，此时就宕机了，会导致内存缓冲里的部分数据会丢失，如果namenode宕机，是可能会导致部分数据丢失的

如果你需要系统保持高性能，你需要允许部分数据的丢失，elasticsearch、redis，都是可能会丢失部分数据的，同步写磁盘，会导致性能很低



2.引入 buffer双缓冲机制，提高editlog刷盘效率，提高系统整体的并发。

FSEditLog不停的往一块缓冲区里去写数据，一旦写满了之后，就由一个线程把这个缓冲区的数据刷入磁盘。

在刷磁盘之前，会做一个缓冲区的交换，把两块缓冲区交换一下，让后续的editslog写入另外一块空置的缓冲区里去，之前写满的一块缓冲区就可以刷入磁盘中了。

双缓冲的思想就是尽可能的缩短锁持有的时间，尤其是对锁的优化这一块，提高多个线程来并发写editslog的效率。



3.采用JSON格式进行EditLog的存储

采用JSON格式原因有两点：

第一：阅读性较高，用于排查问题也比较方便

{“OP”: “MKDIR”, “PATH”: “/usr/warehouse/hive”}

{“OP”: “RM”, “PATH”: “/usr/warehouse/hive”}

{“OP”: “CREATE”, “PATH”: “/usr/warehouse/hive/access.log”}

第二：分布式文件系统，文件目录树对应的磁盘文件，其实不会太大，如果太大的话，也可以分成几个文件来存放，一般来说都还好。所以说不用过于讲究，直接用简单易用的JSON格式，一行放一个editslog就可以了



4.引入BackupNode可以作为冷备份的解决方案

冷备份：每隔一段时间可以去备份一下数据，每次恢复数据就是一部分，有些最新的数据一定是会丢失的

热备份：几乎是实时的在同步数据到其他的机器上去，如果说这台机器宕机，其他机器可以立马切换过来接管所有的操作



5.采用pull模型，BackupNode可以不停的批量拉取NameNode的editslog，每次拉取一小批，比如说一条editslog是50字节，每次可以拉取10条数据，500字节，不到1kb的样子。

对于NameNode来说，不需要在内存里积压和缓冲很多的editslog，NameNode只要需要接收人家拉取日志的调用，此时可以尝试从文件里读取一些editslog发送过去就可以了。

如果说BackupNode发生了checkpoint的操作之后，锁掉了内存文件目录树，此时的话，BackupNode就不会去继续拉取editslog了，对于NameNode而言，此时根本不用去关心BackupNode做了什么事情。



6.引入fsimage格式进行文件目录树磁盘存储，就是直接用fastjson把他转换为一个超大的json，写入磁盘文件就可以了

其实目录文件是没有多少的，假设每天上传5万个新文件，一个新文件就是一个名字，几十个字节，一天下来大概1MB，一个月大概也就是30MB；一年才300多MB；几年可能才一两个GB。



7.采用分布式方式管理dataNode节点，每台dataNode都存储部分数据，每个文件都两个副本冗余，dataNode节点自动上报存储信息到master节点

多个机器之间的负载均衡：master节点必须知道每台机器放了多少数据量的文件，然后把这些机器的数据量的大小进行排序，选择数据量最小的两台机器就可以了。





## 重要接口的流程图



创建文件夹

![image-20221103120434575](/Users/linhaibo/Documents/code/personal/mini-hdfs/README.assets/image-20221103120434575.png)



文件上传

![image-20221103200919358](/Users/linhaibo/Documents/code/personal/mini-hdfs/README.assets/image-20221103200919358.png)

## 重要阶段测试流程

**阶段一：准备整体测试namenode和backupnode的故障重启之后拉取editlog进行回放功能**

1.客户端发送1000条数据给namenode，backupnode同样也拉取到1000条数据。此时有2个磁盘文件+内存缓冲里的部分数据。

2.等待backupnode执行一次checkpoint操作，fsimages -> 1000条数据

3.再发送1000条数据给namenode，此时会多出几个磁盘文件，内存里也有部分数据，fsimage文件中只有1000条数据

4.客户端优雅关闭namenode，调用shutdown接口，namenode会将内存缓冲的数据刷到磁盘上，checkpoint txid 刷入磁盘

5.重启namenode，恢复元数据，backupnode会继续拉取数据。观察日志打印。





**阶段二：测试文件上传的功能**

1.
