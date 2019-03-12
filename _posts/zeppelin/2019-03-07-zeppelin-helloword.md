---
layout: post
comments: false
categories: spark
---

### zeppelin

使用 zeppelin 透過 spark 對 cassandra 做查詢．

先用 docker 下載後，再開 http://localhost:8080

```
docker pull apache/zeppelin:0.8.1
docker run -p 8080:8080 --rm apache/zeppelin:0.8.1
```

建立 notebook

![zeppelin_1.jpg](/static/img/zeppelin/zeppelin_1.jpg){:height="80%" width="80%"}

原本程式的寫法 :  

```
val spark: SparkSession = SparkSession.builder().appName("KmeansLookLikePerson")
      .config("spark.sql.warehouse.dir", "/tmp")
      .config("spark.cassandra.connection.host", EnrichConfig.CASSANDRA_IPS)
      .config("spark.cassandra.auth.username", EnrichConfig.CASSANDRA_USER)
      .config("spark.cassandra.auth.password", EnrichConfig.CASSANDRA_PSWD)
      .getOrCreate()

val allPersons = spark.sparkContext.cassandraTable[(String,Seq[Double])]("miks2","testpersonlabelvector")
  .select("id","labelvector").keyBy[Tuple1[String]]("id")

```

寫到 notebook 裡面，可參考 [spark Interpreter for zeppelin](https://zeppelin.apache.org/docs/0.7.2/interpreter/spark.html#sparkcontext-sqlcontext-sparksession-zeppelincontext)，
所以 zeppelin 預設就會給一個 sc 就是 sparkContext．

```
import com.datastax.spark.connector._

val allPersons = sc.cassandraTable[(String,Seq[Double])]("miks2","testpersonlabelvector")
      .select("id","labelvector").keyBy[Tuple1[String]]("id")

allPersons.take(5).foreach(p => println(p._1._1))

```

那原本要設定 cassandra 的 host、username、password 要帶給 sparkContext，就要設定在 Interpreter 的 spark 設定裡．  

![zeppelin_2.jpg](/static/img/zeppelin/zeppelin_2.jpg){:height="80%" width="80%"}

把參數設定在這裡 :  

![zeppelin_3.jpg](/static/img/zeppelin/zeppelin_3.jpg){:height="80%" width="80%"}

那由於有使用到 spark cassandra 所以要 import library

```
import com.datastax.spark.connector._
```

設定在 dependencies，加上需要的 library．

![zeppelin_4.jpg](/static/img/zeppelin/zeppelin_4.jpg){:height="80%" width="80%"}

zeppelin 會去 maven 或指定的 repositories 下載 library．

![zeppelin_5.jpg](/static/img/zeppelin/zeppelin_5.jpg){:height="80%" width="80%"}

結著只要按 run 就可以了．

![zeppelin_6.jpg](/static/img/zeppelin/zeppelin_6.jpg){:height="80%" width="80%"}


如果需要有一些視覺化的圖表，則需要先把 Dataframe 轉成 spark sql 的 table．  

```
looklikePersons.registerTempTable("looklikePersons")
allPersons.toDF().registerTempTable("allPersons")
seedPersons.toDF().registerTempTable("seedPersons")
```

程式碼如下 :  

```
import com.datastax.spark.connector._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.Vectors

val kcluster = 2
val outputPersons = 10

val testpersonSeed = "hash:9ef58b79-8aab-4e5b-bcf5-b8974991e599,hash:cd5d9c99-393b-4814-8f62-9aebcc31bd21,hash:a0c95fa1-fc44-44ff-b4ae-c407cd53f292,hash:a2ada011-53a1-4d27-9885-dbfd2b0cb969,hash:ce1283e2-a72a-4377-86f5-c03774f00641,hash:4241c150-cde7-4cb7-9306-10946cdbb639,hash:be9fcd09-a1f5-467a-9065-82a89c86af83,hash:48f3d940-f0bd-4300-9f8a-33c29c56ace6,hash:35dc9c3d-f04d-4b27-a5ce-8180ea9705ec,hash:57258600-55c5-4d94-8d15-8f754d8d14bc"

val allPersons = sc.cassandraTable[(String,Seq[Double])]("miks2","testpersonlabelvector")
      .select("id","labelvector").keyBy[Tuple1[String]]("id")

val limitPersons = spark.sparkContext.parallelize(allPersons.take(1000))

val seedPersons = sc.cassandraTable[(String,Seq[Double])]("miks2","testpersonlabelvector")
  .select("id","labelvector").where("id in ? " , testpersonSeed.split(",").toSeq).keyBy[Tuple1[String]]("id")

val tranDataSet = seedPersons.filter(_._2._2.size == 254).map(r => ( r._2._1 ,Vectors.dense(r._2._2.toArray))).toDF("id", "features")

val kmeans = new KMeans().setK(kcluster).setSeed(kcluster)
val model = kmeans.fit(tranDataSet)
val seedKmodel = sc.parallelize(model.clusterCenters)
val resultIdList = seedKmodel.cartesian(limitPersons)
  .map(ds => (ds._2._1._1 , cosineSimilarity(ds._1.toArray , ds._2._2._2)))
  .sortBy(_._2 , false).take(outputPersons)

val looklikePersons = sc.parallelize(resultIdList).toDF()
looklikePersons.registerTempTable("looklikePersons")
allPersons.toDF().registerTempTable("allPersons")
seedPersons.toDF().registerTempTable("seedPersons")

def cosineSimilarity(x: Seq[Double], y: Seq[Double]): Double = {
  val v = genDot(x, y)/(magnitude(x) * magnitude(y))
  if(v.isNaN) {
    0
  } else {
    v
  }
}

def genDot(x: Seq[Double], y: Seq[Double]): Double = {
  (for((a, b) <- x.zip(y)) yield a * b).sum
}

def magnitude(x: Seq[Double]): Double = {
  math.sqrt(x.map(i => i*i).sum)
}
```

接著只要下 sql 按 run 就可以看到圖表

```
%sql
select * from looklikePersons 
```

![zeppelin_7.jpg](/static/img/zeppelin/zeppelin_7.jpg){:height="80%" width="80%"}

使用 spark 查詢 cassandra 然後建立一個 table 建立一個 spark sql 的 table，讓 user 用 sql 下查詢並看結果．  
先查詢要的 table 及欄位 :  

```
import com.datastax.spark.connector._

val allPersons = sc.cassandraTable("miks1","twmperson").select("id","labels").map(row => (row.get[String]("id") , row.get[String]("labels"))).toDF("id","labels")

allPersons.registerTempTable("allPersons")

```

使用 sql 查詢想要的 labels 來查看有多少人有這些 labels  

```
%sql
select id,labels from allPersons where labels like '%36%' or labels like '%92%'
```

結果表示有 labels 36 跟 92 的人共有 241 個。  

![zeppelin_8.jpg](/static/img/zeppelin/zeppelin_8.jpg){:height="80%" width="80%"}





```
./bin/install-interpreter.sh --name "interpreter-name" --artifact org.apache.zeppelin:spark2-shims:0.8.0 
```

```
/zeppelin-0.8.1-bin-all/bin>./install-interpreter.sh --name "interpreter-name" --artifact org.apache.zeppelin:spark2-shims:0.8.0
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=512m; support was removed in 8.0
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/home/miuser/enrich/zeppelin/zeppelin-0.8.1-bin-all/lib/interpreter/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/home/miuser/enrich/zeppelin/zeppelin-0.8.1-bin-all/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
Install interpreter-name(org.apache.zeppelin:spark2-shims:0.8.0) to /home/miuser/enrich/zeppelin/zeppelin-0.8.1-bin-all/interpreter/interpreter-name ...
Interpreter interpreter-name installed under /home/miuser/enrich/zeppelin/zeppelin-0.8.1-bin-all/interpreter/interpreter-name.

1. Restart Zeppelin
2. Create interpreter setting in 'Interpreter' menu on Zeppelin GUI
3. Then you can bind the interpreter on your note
miuser@dmpn1:~/enrich/zeppelin/zeppelin-0.8.1-bin-all/bin>ll
total 33

```

如果環境是 spark 1 是使用 sc :
```
val allPersons = sc.cassandraTable[(String,Map[String,Double])]("miks1","twmperson").select("id","labels").filter(l => l._2.contains("92")).toDF("id","labels")
```
如果環境是 spark 2 記得改成使用 spark(sparksession)，否則會有 dependency 的問題 :

```
import com.datastax.spark.connector._

val allPersons = spark.sparkContext.cassandraTable[(String,Map[String,Double])]("miks1","twmperson").select("id","labels").filter(l => l._2.contains("92")).toDF("id","labels")

allPersons.registerTempTable("allPersons")

```
這段很重要 :  
```
SparkContext, SQLContext and ZeppelinContext are automatically created and exposed as variable names sc, sqlContext and z, respectively, in Scala, Python and R environments. Staring from 0.6.1 SparkSession is available as variable spark when you are using Spark 2.x.
```

遇到下列的錯誤  

```
java.lang.NoSuchMethodError: io.netty.buffer.PooledByteBufAllocator.metric()Lio/netty/buffer/PooledByteBufAllocatorMetric;
  at org.apache.spark.network.util.NettyMemoryMetrics.registerMetrics(NettyMemoryMetrics.java:80)
  at org.apache.spark.network.util.NettyMemoryMetrics.<init>(NettyMemoryMetrics.java:76)
  at org.apache.spark.network.client.TransportClientFactory.<init>(TransportClientFactory.java:109)
  at org.apache.spark.network.TransportContext.createClientFactory(TransportContext.java:99)
  at org.apache.spark.rpc.netty.NettyRpcEnv.<init>(NettyRpcEnv.scala:71)
  at org.apache.spark.rpc.netty.NettyRpcEnvFactory.create(NettyRpcEnv.scala:461)
  at org.apache.spark.rpc.RpcEnv$.create(RpcEnv.scala:57)
  at org.apache.spark.SparkEnv$.create(SparkEnv.scala:249)
  at org.apache.spark.SparkEnv$.createDriverEnv(SparkEnv.scala:175)
  at org.apache.spark.SparkContext.createSparkEnv(SparkContext.scala:256)
  at org.apache.spark.SparkContext.<init>(SparkContext.scala:423)
  at org.apache.spark.SparkContext$.getOrCreate(SparkContext.scala:2493)
  at org.apache.spark.sql.SparkSession$Builder$$anonfun$7.apply(SparkSession.scala:933)
  at org.apache.spark.sql.SparkSession$Builder$$anonfun$7.apply(SparkSession.scala:924)
  at scala.Option.getOrElse(Option.scala:121)
  at org.apache.spark.sql.SparkSession$Builder.getOrCreate(SparkSession.scala:924)
  at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
  at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
  at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
  at java.lang.reflect.Method.invoke(Method.java:498)
  at org.apache.zeppelin.spark.BaseSparkScalaInterpreter.spark2CreateContext(BaseSparkScalaInterpreter.scala:259)
  at org.apache.zeppelin.spark.BaseSparkScalaInterpreter.createSparkContext(BaseSparkScalaInterpreter.scala:178)
  at org.apache.zeppelin.spark.SparkScala211Interpreter.open(SparkScala211Interpreter.scala:89)
  at org.apache.zeppelin.spark.NewSparkInterpreter.open(NewSparkInterpreter.java:102)
  at org.apache.zeppelin.spark.SparkInterpreter.open(SparkInterpreter.java:62)
  at org.apache.zeppelin.interpreter.LazyOpenInterpreter.open(LazyOpenInterpreter.java:69)
  at org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer$InterpretJob.jobRun(RemoteInterpreterServer.java:616)
  at org.apache.zeppelin.scheduler.Job.run(Job.java:188)
  at org.apache.zeppelin.scheduler.FIFOScheduler$1.run(FIFOScheduler.java:140)
  at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
  at java.util.concurrent.FutureTask.run(FutureTask.java:266)
  at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180)
  at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
  at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
  at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
  at java.lang.Thread.run(Thread.java:748)
```


zeppelin netty jar 的版本 :  
```
/zeppelin/zeppelin-0.8.1-bin-all/lib>ll | grep netty-all
-rw-r--r--  1 miuser miuser  1779991 Oct 16 11:05 netty-all-4.0.23.Final.jar
```

spark-2.3.1 netty jar 的版本 :  

```
/opt/spark-2.3.1-bin-hadoop2.7/jars>ll | grep netty-all
-rw-rw-r--  1 miuser miuser  3780056 Jun  2  2018 netty-all-4.1.17.Final.jar
```

所以把 spark 的 netty jar copy 到 zeppelin


接著遇到 Jackson 版本問題，一樣把 spark 的 jackson jar 覆蓋到 zeppelin．

```
com.fasterxml.jackson.databind.JsonMappingException: Incompatible Jackson version: 2.8.11-1
  at com.fasterxml.jackson.module.scala.JacksonModule$class.setupModule(JacksonModule.scala:64)
  at com.fasterxml.jackson.module.scala.DefaultScalaModule.setupModule(DefaultScalaModule.scala:19)
  at com.fasterxml.jackson.databind.ObjectMapper.registerModule(ObjectMapper.java:747)
  at org.apache.spark.util.JsonProtocol$.<init>(JsonProtocol.scala:59)
  at org.apache.spark.util.JsonProtocol$.<clinit>(JsonProtocol.scala)
  at org.apache.spark.scheduler.EventLoggingListener$.initEventLog(EventLoggingListener.scala:303)
  at org.apache.spark.scheduler.EventLoggingListener.start(EventLoggingListener.scala:128)
  at org.apache.spark.SparkContext.<init>(SparkContext.scala:522)
  at org.apache.spark.SparkContext$.getOrCreate(SparkContext.scala:2493)
  at org.apache.spark.sql.SparkSession$Builder$$anonfun$7.apply(SparkSession.scala:933)
  at org.apache.spark.sql.SparkSession$Builder$$anonfun$7.apply(SparkSession.scala:924)
  at scala.Option.getOrElse(Option.scala:121)
  at org.apache.spark.sql.SparkSession$Builder.getOrCreate(SparkSession.scala:924)
  at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
  at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
  at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
  at java.lang.reflect.Method.invoke(Method.java:498)
  at org.apache.zeppelin.spark.BaseSparkScalaInterpreter.spark2CreateContext(BaseSparkScalaInterpreter.scala:259)
  at org.apache.zeppelin.spark.BaseSparkScalaInterpreter.createSparkContext(BaseSparkScalaInterpreter.scala:178)
  at org.apache.zeppelin.spark.SparkScala211Interpreter.open(SparkScala211Interpreter.scala:89)
  at org.apache.zeppelin.spark.NewSparkInterpreter.open(NewSparkInterpreter.java:102)
  at org.apache.zeppelin.spark.SparkInterpreter.open(SparkInterpreter.java:62)
  at org.apache.zeppelin.interpreter.LazyOpenInterpreter.open(LazyOpenInterpreter.java:69)
  at org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer$InterpretJob.jobRun(RemoteInterpreterServer.java:616)
  at org.apache.zeppelin.scheduler.Job.run(Job.java:188)
  at org.apache.zeppelin.scheduler.FIFOScheduler$1.run(FIFOScheduler.java:140)
  at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
  at java.util.concurrent.FutureTask.run(FutureTask.java:266)
  at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180)
  at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
  at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
  at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
  at java.lang.Thread.run(Thread.java:748)
```


```
/zeppelin/zeppelin-0.8.1-bin-all/lib>ll | grep jackson
-rw-r--r--  1 miuser miuser     5990 Oct 16 14:40 google-http-client-jackson-1.23.0.jar
-rw-r--r--  1 miuser miuser     6675 Oct 16 14:40 google-http-client-jackson2-1.23.0.jar
-rw-r--r--  1 miuser miuser    55784 Oct 16 11:58 jackson-annotations-2.8.0.jar
-rw-r--r--  1 miuser miuser   282634 Oct 16 14:38 jackson-core-2.8.10.jar
-rw-r--r--  1 miuser miuser   232248 Oct 16 10:56 jackson-core-asl-1.9.13.jar
-rw-r--r--  1 miuser miuser  1247444 Oct 16 14:38 jackson-databind-2.8.11.1.jar
-rw-r--r--  1 miuser miuser    18336 Oct 16 10:57 jackson-jaxrs-1.9.13.jar
-rw-r--r--  1 miuser miuser   780664 Oct 16 10:56 jackson-mapper-asl-1.9.13.jar
-rw-r--r--  1 miuser miuser    34610 Oct 16 14:38 jackson-module-jaxb-annotations-2.8.10.jar
-rw-r--r--  1 miuser miuser    27084 Oct 16 10:57 jackson-xc-1.9.13.jar
-rw-r--r--  1 miuser miuser    73055 Oct 16 14:38 jersey-media-json-jackson-2.27.jar
/zeppelin/zeppelin-0.8.1-bin-all/lib>rm -f jackson-*
/zeppelin/zeppelin-0.8.1-bin-all/lib>cp /opt/spark-2.3.1-bin-hadoop2.7/jars/jackson-* .
/zeppelin/zeppelin-0.8.1-bin-all/lib>ll | grep jackson
-rw-r--r--  1 miuser miuser     5990 Oct 16 14:40 google-http-client-jackson-1.23.0.jar
-rw-r--r--  1 miuser miuser     6675 Oct 16 14:40 google-http-client-jackson2-1.23.0.jar
-rw-rw-r--  1 miuser miuser    46986 Mar 12 11:35 jackson-annotations-2.6.7.jar
-rw-rw-r--  1 miuser miuser   258919 Mar 12 11:35 jackson-core-2.6.7.jar
-rw-rw-r--  1 miuser miuser   232248 Mar 12 11:35 jackson-core-asl-1.9.13.jar
-rw-rw-r--  1 miuser miuser  1165323 Mar 12 11:35 jackson-databind-2.6.7.1.jar
-rw-rw-r--  1 miuser miuser   320444 Mar 12 11:35 jackson-dataformat-yaml-2.6.7.jar
-rw-rw-r--  1 miuser miuser    18336 Mar 12 11:35 jackson-jaxrs-1.9.13.jar
-rw-rw-r--  1 miuser miuser   780664 Mar 12 11:35 jackson-mapper-asl-1.9.13.jar
-rw-rw-r--  1 miuser miuser    32612 Mar 12 11:35 jackson-module-jaxb-annotations-2.6.7.jar
-rw-rw-r--  1 miuser miuser    42858 Mar 12 11:35 jackson-module-paranamer-2.7.9.jar
-rw-rw-r--  1 miuser miuser   515645 Mar 12 11:35 jackson-module-scala_2.11-2.6.7.1.jar
-rw-rw-r--  1 miuser miuser    27084 Mar 12 11:35 jackson-xc-1.9.13.jar
-rw-r--r--  1 miuser miuser    73055 Oct 16 14:38 jersey-media-json-jackson-2.27.jar
```




[zeppelin 整 spark 問題](https://blog.csdn.net/a376554764/article/details/84672444)




