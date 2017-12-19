mapreduce程序运行的几种模式
1、纯本地模式（mapreduce以及所处理的数据都在本地）
纯粹建一个干净工程，写好mapper类和reducer类和runner类，然后运行main方法即可

2、半本地模式（mapreduce是在本地运行，访问的数据可以在hdfs集群上）
跟上面一种模式没有太大区别，唯有一点，最好要将core-site.xml和hdfs-site.xml放到工程的src目录下

3、提交到集群运行的模式a(适合在linux系统中做)
是在IDEA里面运行runner类的main方法，但确实提交到集群上去运行了
要实现这种运行方法，要将core-site.xml和hdfs-site.xml、mapred-site.xml、yarn-site.xml放到src目录下

4、提交到集群运行的模式b
通过hadoop  jar xxx.jar 权限类 输入路径 输出路径 的命令来提交
