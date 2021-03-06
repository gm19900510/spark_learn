# hive2_on_spark

利用spark2操作hive，适用于Spark2.X版本


## Spark版本说明


- 本项目`Spark`版本说明

  基于`Spark2.4.6`源码编译而成，`Hadoop`版本为`2.6.0-cdh5.16.2`

  源码编译请参照：

  https://gaoming.blog.csdn.net/article/details/108203548

    ***本地依赖的JAR必须与集群组件版本匹配***

- 基于`Maven`管理已发布`Spark`版本说明

  示例：

  ```xml
  <properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <maven.compiler.source>1.8</maven.compiler.source>
    <maven.compiler.target>1.8</maven.compiler.target>
    <spark.version>2.1.0</spark.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-streaming-kafka-0-10_2.11</artifactId>
      <version>${spark.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-streaming_2.11</artifactId>
      <version>${spark.version}</version>
    </dependency>
    <dependency>
      <groupId>com.mchange</groupId>
      <artifactId>c3p0</artifactId>
      <version>0.9.5-pre3</version>
    </dependency>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-core_2.11</artifactId>
      <version>2.1.1</version>
    </dependency>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-sql_2.11</artifactId>
      <version>2.1.1</version>
    </dependency>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-hive_2.11</artifactId>
      <version>2.1.1</version>
      </dependency>
    <dependency>
      <groupId>org.spark-project.hive</groupId>
      <artifactId>hive-exec</artifactId>
      <version>1.2.1.spark2</version>
    </dependency>
    <dependency>
      <groupId>joda-time</groupId>
      <artifactId>joda-time</artifactId>
      <version>2.9.9</version>
    </dependency>
  </dependencies>
  ```
## 集群环境说明

IP     | HOSTNAME 
-------- | -----
192.168.104.251  | localhost
192.168.3.163  | T630
192.168.3.197  | S0
192.168.3.18  | S1
192.168.3.161  | S2
192.168.3.136  | S3


## Spark部署方式

- Yarn模式

  `mvn package`

  - 测试 `Demo_Context_Mode`
  
    `spark-submit --master yarn --deploy-mode client --class com.gm.hive.Demo_Context_Mode /root/spark2_on_hive-0.0.1-SNAPSHOT-jar-with-dependencies.jar`

    `spark-submit --master yarn --deploy-mode cluster --class com.gm.hive.Demo_Context_Mode /root/spark2_on_hive-0.0.1-SNAPSHOT-jar-with-dependencies.jar`

  - 测试 `Demo_Session_Mode`
  
    `spark-submit --master yarn --deploy-mode client --class com.gm.hive.Demo_Session_Mode /root/spark2_on_hive-0.0.1-SNAPSHOT-jar-with-dependencies.jar`

    `spark-submit --master yarn --deploy-mode cluster --class com.gm.hive.Demo_Session_Mode /root/spark2_on_hive-0.0.1-SNAPSHOT-jar-with-dependencies.jar`

- Standalone模式

  `mvn package`
  - 测试 `Demo_Context_Mode`
    
    `spark-submit --master spark://T630:7077 --class com.gm.hive.Demo_Context_Mode /root/spark2_on_hive-0.0.1-SNAPSHOT-jar-with-dependencies.jar`

  - 测试 `Demo_Session_Mode`
  
    `spark-submit --master spark://T630:7077 --class com.gm.hive.Demo_Session_Mode /root/spark2_on_hive-0.0.1-SNAPSHOT-jar-with-dependencies.jar`

- Local模式

  `mvn package`
  - 测试 `Demo_Context_Mode` 
  
    `spark-submit --master local[2] --class com.gm.hive.Demo_Context_Mode /root/spark2_on_hive-0.0.1-SNAPSHOT-jar-with-dependencies.jar`

  - 测试 `Demo_Session_Mode`

    `spark-submit --master local[2] --class com.gm.hive.Demo_Session_Mode /root/spark2_on_hive-0.0.1-SNAPSHOT-jar-with-dependencies.jar`

- java -jar或java -cp
 
  `mvn package`
  
  `java -jar spark2_on_hive-0.0.1-SNAPSHOT-release.jar`

  记得在`pox.xml`中更改`mainClass`

- share lib 
 
  `mvn package`
  
  在`Hue、 Ozzie`下使用 `spark2_on_hive-0.0.1-SNAPSHOT.jar`

  请参照：https://gaoming.blog.csdn.net/article/details/107933726 

## 重要说明

- 本地`Run Java Application`运行程序访问Hive时（未使用`spark-submit`方式）须在项目的`resources`目录放置`hive-site.xml`文件，未放置会出现未发现元数据异常。

-  `enableHiveSupport()`开启`Hive`支持。

-  `conf.set("spark.driver.host", "192.168.104.251");`设置本地的`host`防止出现通过`hostname`无法访问主机的问题。



## `java -jar`运行时出现的问题及解决方案汇总

- `java.io.IOException: No FileSystem for scheme: file`
  
    原因：
    
    在`hadoop-commons`和`hadoop-hdfs`两个`jar`文件中，在`META-INFO/services`下包含相同的文件名`org.apache.hadoop.fs.FileSystem`，而我们使用`maven-assembly-plugin`时，最终只有一个文件被保留，所以被重写的那个文件系统就无法找到。

    解决方案：
    ```java
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    Configuration h_conf = sc.hadoopConfiguration();
    h_conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    h_conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    ```

- `com.typesafe.config.ConfigException$Missing: No configuration setting found for key 'akka.version'`
  
    原因：
    
    `Akka`的配置方法在很大程度上依赖于每个模块/jar都有自己的`reference.conf`文件的概念，所有这些都将由配置发现并加载。不幸的是，这也意味着如果你将多个`jar`放入/合并到同一个`jar`中，你也需要合并所有的`reference.confs`。否则所有默认值都将丢失，`Akka`将无法运行

    解决方案：
    
    在`pom.xml`中使用`maven-shade-plugin`插件
    
    ```java
    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>2.3</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals>
              <goal>shade</goal>
            </goals>
            <configuration>
              <shadedArtifactAttached>true</shadedArtifactAttached>
              <shadedClassifierName>allinone</shadedClassifierName>
              <artifactSet>
                <includes>
                  <include>*:*</include>
                </includes>
              </artifactSet>
              <transformers>
                <transformer
                  implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                  <resource>reference.conf</resource>
                </transformer>
                <transformer
                  implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <manifestEntries>
                    <Main-Class>com.gm.hive.Demo</Main-Class>
                  </manifestEntries>
                </transformer>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
    ``` 

    此时打完包后还会存在以下问题
    ``` 
    Exception in thread "main" java.lang.SecurityException: Invalid signature file digest for Manifest main attributes
    ``` 
    
    原因：
    
    在打包时更改了`MANIFEST.MF`内容，导致它跟原先的`jar`包签名不符，导致校验失败，程序无法运行

    解决方案：

    打包时过滤掉 `*.RSA, *.SF, *.DSA`文件

    完整版`maven-shade-plugin`配置如下：
    ```xml 
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-shade-plugin</artifactId>
      <version>2.3</version>
      <executions>
        <execution>
          <phase>package</phase>
          <goals>
            <goal>shade</goal>
          </goals>
          <configuration>
            <shadedArtifactAttached>true</shadedArtifactAttached>
            <shadedClassifierName>allinone</shadedClassifierName>
            <artifactSet>
              <includes>
                <include>*:*</include>
              </includes>
            </artifactSet>
            <transformers>
              <transformer
                implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                <resource>reference.conf</resource>
              </transformer>
              <transformer
                implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                <manifestEntries>
                  <Main-Class>com.gm.hive.Demo</Main-Class>
                </manifestEntries>
              </transformer>
            </transformers>
            <filters>
                              <filter>
                                  <artifact>*:*</artifact>
                                  <excludes>
                                      <exclude>META-INF/*.SF</exclude>
                                      <exclude>META-INF/*.DSA</exclude>
                                      <exclude>META-INF/*.RSA</exclude>
                                  </excludes>
                              </filter>
                          </filters>
          </configuration>
        </execution>
      </executions>
    </plugin>
    ```

- `java.lang.ClassNotFoundException: Failed to find data source: json`   

    解决方案：
    ```java
    df_result.write().mode(SaveMode.Overwrite).json("hdfs://s0:8020/input/df_result");
    ```
    将以上代码改为以下部分，指定`OutputFormat`的输出格式的`class`
    ```java
    df_result.write().format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat").mode(SaveMode.Overwrite).save("hdfs://s0:8020/input/df_result");
    ```



