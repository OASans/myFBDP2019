#README

## 金融大数据处理技术-作业4

### 1 Hadoop3.2.1伪分布式+IntelliJ IDEA环境配置

首先在mac os中通过homebrew下载hadoop3.2.1，并修改好hadoop的配置文件，具体步骤与Ubuntu系统类似，不再赘述。

新建IDEA项目时选择Maven项目

<div align=center><img src="/Users/ishi/Desktop/Screenshot 2019-10-22 at 16.11.05.png" width="800"/></div>

在项目的pom.xml中根据个人hadoop版本，添加配置

```
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>oasans.com</groupId>
    <artifactId>OASans</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>RELEASE</version>
        </dependency>
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-core</artifactId>
            <version>2.8.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>3.2.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>3.2.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs</artifactId>
            <version>3.2.1</version>
        </dependency>
    </dependencies>

</project>
```

在`Project Structure-Modules`中导入Hadoop配置中的jar包作为依赖

![image-20191022161725205](/Users/ishi/Library/Application Support/typora-user-images/image-20191022161725205.png)

在resources中导入Hadoop配置文件coresite.xml、hdfs-site.xml

在main下编写好代码后，在`Run-Edit Configurations`中新建Application，选择代码运行主类，并在Program arguments中输入HDFS文件系统中输入输出文件的路径以及其他参数。此后便可正常运行。

![image-20191022161926726](/Users/ishi/Library/Application Support/typora-user-images/image-20191022161926726.png)



### 2 代码使用方法

####2.1 给出矩阵乘法的MapReduce实现，以M_3_4和N_4_2作为输入进行测试。

代码位于`171860015-石霭青-作业4/代码/homework4_1/`文件夹中，`Run-Edit Configurations`中的参数如下

* **Main class**：MatrixMultiply

* **Program arguments**：\<Matrix M input path> \<Matrix N input path> \<output path>

  示例：/M_3_4 /N_4_2 /output_matrixmultiply

#### 2.2 给出关系代数的选择、投影、并集、交集、差集及自然连接的MapReduce实现。测试集如下：

- 关系Ra：(id, name, age, weight)
- 关系Rb：(id, gender, height)

##### 2.2.1在Ra.txt上选择age=18的记录；在Ra.txt上选择age<18的记录

代码位于`171860015-石霭青-作业4/代码/homework4_2_1_1/`和`171860015-石霭青-作业4/代码/homework4_2_1_2/`文件夹中，`Run-Edit Configurations`中的参数如下

- **Main class**：Selection

- **Program arguments**：<input path\> <output path\> <id\> <value\>

  示例：/Ra /output_selectionage1 2 18

##### 2.2.2 在Ra.txt上对属性name进行投影

代码位于`171860015-石霭青-作业4/代码/homework4_2_2/`文件夹中，`Run-Edit Configurations`中参数如下

- **Main class**：Projection

- **Program arguments**：<input path\> <output path\> <col id\>

  示例：/Ra /output_projection 1

##### 2.2.3 求Ra1和Ra2的并集

代码位于`171860015-石霭青-作业4/代码/homework4_2_3/`文件夹中，`Run-Edit Configurations`中参数如下

- **Main class**：Union

- **Program arguments**：<input path\> <output path\>

  示例：/input_union /output_union

##### 2.2.4 求Ra1和Ra2的交集

代码位于`171860015-石霭青-作业4/代码/homework4_2_4/`文件夹中，`Run-Edit Configurations`中参数如下

- **Main class**：Intersection

- **Program arguments**：<input path\> <output path\>

  示例：/input_intersection /output_intersection

##### 2.2.5 求Ra2 - Ra1

代码位于`171860015-石霭青-作业4/代码/homework4_2_5/`文件夹中，`Run-Edit Configurations`中参数如下

- **Main class**：Difference

- **Program arguments**：<input path\> <output path\> <relation name\>

  示例：/input_difference /output_difference Ra1.txt

##### 2.2.6 Ra和Rb在属性id上进行自然连接，要求最后的输出格式为(id, name, age, gender, weight, height)

代码位于`171860015-石霭青-作业4/代码/homework4_2_6/`文件夹中，`Run-Edit Configurations`中参数如下

- **Main class**：NaturalJoin

- **Program arguments**：<input path\> <output path\> <join col id\> <relation name>

  示例：/input_join /output_join 0 Ra.txt