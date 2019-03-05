---
layout: post
comments: false
categories: spark
---

### spark k-means

Seed 代表一開始初始化 random 的點．K 表示 cluster 的數量．setInitMode 目前支持 random 和 `k-means||`，`k-means||` 是 k-means++．

```
val spark = SparkSession.builder().master("local[*]").appName("testtest").config("spark.cassandra.connection.host", "192.168.6.31").getOrCreate()
val dataset = spark.createDataFrame(Seq(
  (0, Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (2, 1.0)))), // [1.0 , 1.0 , 1.0 , 0   , 0   , 0  ]
  (1, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (4, 1.0)))), // [0   , 0   , 1.0 , 1.0 , 1.0 , 0  ]
  (2, Vectors.sparse(6, Seq((0, 1.0), (2, 1.0), (4, 1.0)))), // [1.0 , 0   , 1.0 , 0   , 1.0 , 0  ]
  (3, Vectors.sparse(6, Seq((1, 1.0), (3, 1.0), (4, 1.0)))), // [0   , 1.0 , 0   , 1.0 , 1.0 , 0  ]
  (4, Vectors.sparse(6, Seq((1, 1.0), (3, 1.0), (5, 1.0)))), // [0   , 1.0 , 0   , 1.0 , 0   , 1.0]
  (5, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (5, 1.0)))), // [0   , 0   , 1.0 , 1.0 , 0   , 1.0]
  (6, Vectors.sparse(6, Seq((1, 1.0), (2, 1.0), (4, 1.0)))), // [0   , 1.0 , 1.0 , 0   , 1.0 , 0  ]
  (7, Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (3, 1.0))))  // [1.0 , 1.0 , 0   , 1.0 , 0   , 0  ]
)).toDF("id", "features")

// Trains a k-means model.
val kmeans = new KMeans().setK(3).setSeed(1L)

val model = kmeans.fit(dataset)

// Make predictions
val predictions = model.transform(dataset)

// Evaluate clustering by computing Silhouette score
val evaluator = new ClusteringEvaluator()

val silhouette = evaluator.evaluate(predictions)
println(s"Silhouette with squared euclidean distance = $silhouette")

// Shows the result.
println("Cluster Centers: ")
model.clusterCenters.foreach(println)

predictions.show()
```

印出結果 : 

```
Silhouette with squared euclidean distance = 0.11111111111111134
Cluster Centers: 
[0.3333333333333333,0.6666666666666666,0.3333333333333333,1.0,0.0,0.6666666666666666]
[0.3333333333333333,0.3333333333333333,0.6666666666666666,0.6666666666666666,1.0,0.0]
[0.5,1.0,1.0,0.0,0.5,0.0]
+---+--------------------+----------+
| id|            features|prediction|
+---+--------------------+----------+
|  0|(6,[0,1,2],[1.0,1...|         2|
|  1|(6,[2,3,4],[1.0,1...|         1|
|  2|(6,[0,2,4],[1.0,1...|         1|
|  3|(6,[1,3,4],[1.0,1...|         1|
|  4|(6,[1,3,5],[1.0,1...|         0|
|  5|(6,[2,3,5],[1.0,1...|         0|
|  6|(6,[1,2,4],[1.0,1...|         2|
|  7|(6,[0,1,3],[1.0,1...|         0|
+---+--------------------+----------+

```
實際在餵給 KMeans 的資料集時，會需要一個 features 的欄位，可透過 Vectors.dense 來將 Array[Double] 轉成 Vector．

```
seedPersons.map(r => ( r._2._1 ,Vectors.dense(r._2._2.toArray))).toDF("id", "features")
```

```
val outputPersons = 100
//10 count
val testpersonSeed = "hash:9ef58b79-8aab-4e5b-bcf5-b8974991e599,hash:cd5d9c99-393b-4814-8f62-9aebcc31bd21,hash:a0c95fa1-fc44-44ff-b4ae-c407cd53f292,hash:a2ada011-53a1-4d27-9885-dbfd2b0cb969,hash:ce1283e2-a72a-4377-86f5-c03774f00641,hash:4241c150-cde7-4cb7-9306-10946cdbb639,hash:be9fcd09-a1f5-467a-9065-82a89c86af83,hash:48f3d940-f0bd-4300-9f8a-33c29c56ace6,hash:35dc9c3d-f04d-4b27-a5ce-8180ea9705ec,hash:57258600-55c5-4d94-8d15-8f754d8d14bc"

val inputIdList = testpersonSeed
val spark = SparkSession.builder()
      .appName("LookLikePersonLSH")
      .master("local[*]")
      .config("spark.cassandra.connection.host", "192.168.6.31,192.168.6.32,192.168.6.33")
      .config("spark.cassandra.auth.username", "miuser")
      .config("spark.cassandra.auth.password", "mimimi123")
      .getOrCreate()

import spark.implicits._

val allPersons = spark.sparkContext.cassandraTable[(String,Seq[Double])]("miks2","testpersonlabelvector")
  .select("id","labelvector").keyBy[Tuple1[String]]("id")

val seedPersons = spark.sparkContext.cassandraTable[(String,Seq[Double])]("miks2","testpersonlabelvector")
  .select("id","labelvector").where("id in ? " , testpersonSeed.split(",").toSeq).keyBy[Tuple1[String]]("id")

val tranDataSet = seedPersons.map(r => ( r._2._1 ,Vectors.dense(r._2._2.toArray))).toDF("id", "features")

val kmeans = new KMeans().setK(5).setSeed(5L)
val model = kmeans.fit(tranDataSet)

val seedKmodel = spark.sparkContext.parallelize(model.clusterCenters)
val resultIdList = seedKmodel.cartesian(allPersons)
  .map(ds => (ds._2._1._1 , LabelVectorUtil().cosineSimilarity(ds._1.toArray , ds._2._2._2)))
  .sortBy(_._2 , false).take(outputPersons)

spark.sparkContext.parallelize(resultIdList).saveAsTextFile("/enrich/tempResult/KmeansLookLikePerson")
```

跑 Kmeans 如果遇到下列錯誤時，可能因為丟給 KMeans 裡的向量的維度有的有不一樣，可以先檢查 data 的部分．

```
User class threw exception: org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 3.0 failed 4 times, most recent failure: Lost task 0.3 in stage 3.0 (TID 7, dmpn1, executor 5): java.lang.IllegalArgumentException: requirement failed
at scala.Predef$.require(Predef.scala:212)
at org.apache.spark.mllib.util.MLUtils$.fastSquaredDistance(MLUtils.scala:507)
at org.apache.spark.mllib.clustering.KMeans$.fastSquaredDistance(KMeans.scala:590)
at org.apache.spark.mllib.clustering.KMeans$$anonfun$findClosest$1.apply(KMeans.scala:564)
at org.apache.spark.mllib.clustering.KMeans$$anonfun$findClosest$1.apply(KMeans.scala:558)
at scala.collection.IndexedSeqOptimized$class.foreach(IndexedSeqOptimized.scala:33)
at scala.collection.mutable.ArrayOps$ofRef.foreach(ArrayOps.scala:186)
at org.apache.spark.mllib.clustering.KMeans$.findClosest(KMeans.scala:558)
at org.apache.spark.mllib.clustering.KMeans$$anonfun$6$$anonfun$apply$2.apply(KMeans.scala:284)
at org.apache.spark.mllib.clustering.KMeans$$anonfun$6$$anonfun$apply$2.apply(KMeans.scala:283)
at scala.collection.Iterator$class.foreach(Iterator.scala:893)
...
```



```
package ght.mi.ml

import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object KmeansLookLikePerson {

  def main(args: Array[String]): Unit = {

    val outputPersons = args(0).toInt
    val hdfsCsvFile = args(1)
    val kcluster = args(2).toInt
    val jobkey = args(3)

    //val testpersonSeed = "hash:9ef58b79-8aab-4e5b-bcf5-b8974991e599,hash:cd5d9c99-393b-4814-8f62-9aebcc31bd21,hash:a0c95fa1-fc44-44ff-b4ae-c407cd53f292,hash:a2ada011-53a1-4d27-9885-dbfd2b0cb969,hash:ce1283e2-a72a-4377-86f5-c03774f00641,hash:4241c150-cde7-4cb7-9306-10946cdbb639,hash:be9fcd09-a1f5-467a-9065-82a89c86af83,hash:48f3d940-f0bd-4300-9f8a-33c29c56ace6,hash:35dc9c3d-f04d-4b27-a5ce-8180ea9705ec,hash:57258600-55c5-4d94-8d15-8f754d8d14bc"

    val spark = SparkSession.builder()
      .appName("KmeansLookLikePerson")
      .config("spark.cassandra.connection.host", "192.168.6.31,192.168.6.32,192.168.6.33")
      .config("spark.cassandra.auth.username", "miuser")
      .config("spark.cassandra.auth.password", "mimimi123")
      .getOrCreate()

    val sparkId = spark.sparkContext.applicationId
    spark.sparkContext.parallelize(Seq((jobkey , sparkId , "0"))).saveToCassandra("miks2" , "joblist")

    import spark.implicits._

    val customSchema = StructType(Array(StructField("id", StringType, true)))
    // val df = spark.sqlContext.read.format("com.databricks.spark.csv").schema(customSchema).load("/enrich/looklike/idlist/idlist.csv")
    val df = spark.sqlContext.read.format("com.databricks.spark.csv").schema(customSchema).load(hdfsCsvFile)

    val testpersonSeed = df.map(r => r.getAs[String]("id")).collect().mkString(",")

    val allPersons = spark.sparkContext.cassandraTable[(String,Seq[Double])]("miks2","testpersonlabelvector")
      .select("id","labelvector").keyBy[Tuple1[String]]("id")

    val seedPersons = spark.sparkContext.cassandraTable[(String,Seq[Double])]("miks2","testpersonlabelvector")
      .select("id","labelvector").where("id in ? " , testpersonSeed.split(",").toSeq).keyBy[Tuple1[String]]("id")

    val tranDataSet = seedPersons.filter(_._2._2.size == 254).map(r => ( r._2._1 ,Vectors.dense(r._2._2.toArray))).toDF("id", "features")

    /* check label count
    hash:224d426a-ad9b-4e76-b1ef-3e37c963f14e , 1524
    hash:23c7bc27-2b22-43fb-be40-2ec8ac94aaca , 1524
    hash:2bc7399b-d36f-4269-be4d-e37e2ad4c5ff , 2032
    hash:4c39e4f3-cf27-4eb3-8fbb-e82a61cde043 , 2032
    hash:6319b6cb-e3fa-40ed-bd24-e1dc3ac9450a , 2032
    val tranDataSet = seedPersons.filter(_._2._2.size != 254).map(r => ( r._2._1 ,Vectors.dense(r._2._2.toArray))).toDF("id", "features")
    tranDataSet.foreach(r => println(r.getAs[String]("id") + " , " + r.getAs[DenseVector]("features").values.size ))
    */

    val kmeans = new KMeans().setK(kcluster).setSeed(kcluster)
    val model = kmeans.fit(tranDataSet)
    val seedKmodel = spark.sparkContext.parallelize(model.clusterCenters)
    val resultIdList = seedKmodel.cartesian(allPersons)
      .map(ds => (ds._2._1._1 , LabelVectorUtil().cosineSimilarity(ds._1.toArray , ds._2._2._2)))
      .sortBy(_._2 , false).take(outputPersons)

    spark.sparkContext.parallelize(resultIdList).saveAsTextFile("/enrich/tempResult/KmeansLookLikePerson")

    spark.sparkContext.parallelize(Seq((jobkey , sparkId , "1"))).saveToCassandra("miks2" , "joblist")

  }
}

```

build.sbt

```
import scalapb.compiler.Version.scalapbVersion

name := "enrich-5.0"

scalaVersion := "2.11.12" // spark only support scala 2.11

PB.targets in Compile := Seq(
    scalapb.gen() -> (sourceManaged in Compile).value
)

//val sparkVersion = "2.4.0"
val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
    "org.apache.spark"       %%  "spark-sql"                   % sparkVersion      % "provided",
    "org.apache.spark"       %%  "spark-core"                  % sparkVersion      % "provided",
    "org.apache.spark"       %%  "spark-streaming"             % sparkVersion      % "provided",
    "org.apache.spark"       %%  "spark-streaming-kafka-0-10"  % sparkVersion      % "provided" exclude("net.jpountz.lz4", "lz4"),
    "org.apache.spark"       %%  "spark-mllib"                 % sparkVersion,
    "org.mongodb.scala"      %%  "mongo-scala-driver"          % "2.3.0",
    "com.datastax.spark"     %%  "spark-cassandra-connector"   % "2.3.2",
    "org.scalatest"          %%  "scalatest"                   % "3.0.5"           % "test",
    "com.thesamet.scalapb"   %%  "scalapb-runtime-grpc"        % scalapbVersion,
    "com.thesamet.scalapb"   %%  "scalapb-runtime"             % scalapbVersion    % "protobuf",
    "io.grpc"                %   "grpc-okhttp"                 % "1.14.0",
    "redis.clients"          %   "jedis"                       % "2.9.0",
    "com.redislabs"          %   "spark-redis"                 % "2.3.1-M2",
    "commons-codec"          %   "commons-codec"               % "1.11",
    "com.jcraft"             %   "jsch"                        % "0.1.53",
    "com.databricks"         %%  "spark-csv"                   % "1.5.0"
)

parallelExecution in Test := false

assemblyJarName in assembly := name.value + ".jar"

test in assembly := {}

assemblyMergeStrategy in assembly := {
    case PathList("ght","mi","cht" , xs @ _*) => MergeStrategy.discard
    //    case PathList("ght","mi","imx" , xs @ _*) => MergeStrategy.discard
    case PathList("ght","mi","twm" , xs @ _*) => MergeStrategy.discard
    case PathList("ght","mi","grpc" , xs @ _*) => MergeStrategy.discard
    case PathList("ght","mi","model","proto" , xs @ _*) => MergeStrategy.discard
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
}

scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation")

enablePlugins(SbtProguard)
proguardOptions in Proguard += "-dontoptimize"
proguardOptions in Proguard ++= Seq("-dontnote", "-dontwarn", "-ignorewarnings","-keepattributes Signature,*Annotation*","-keep class io.** { *; }","-keep class scala.** { *; }","-keep class com.** { *; }","-keep class org.** { *; }","-keep class scalapb.** { *; }")
proguardInputs in Proguard := (dependencyClasspath in Compile).value.files
proguardFilteredInputs in Proguard ++= ProguardOptions.noFilter((packageBin in Compile).value)
proguardOptions in Proguard += ProguardOptions.keepMain("ght.**")

javaOptions in (Proguard, proguard) := Seq("-Xmx4G")

proguardMerge in Proguard := true
proguardMergeStrategies in Proguard += ProguardMerge.last("javax.inject-2.4.0-b34.jar")
proguardMergeStrategies in Proguard += ProguardMerge.last("io/netty/.".r)
proguardMergeStrategies in Proguard += ProguardMerge.last("org/apache/commons/.".r)
proguardMergeStrategies in Proguard += ProguardMerge.last("org/apache/hadoop/.".r)
proguardMergeStrategies in Proguard += ProguardMerge.last("org/apache/spark/.".r)
proguardMergeStrategies in Proguard += ProguardMerge.last("org/aopalliance/.".r)
proguardMergeStrategies in Proguard += ProguardMerge.last("javax/.".r)
proguardMergeStrategies in Proguard += ProguardMerge.last("java/.".r)
proguardMergeStrategies in Proguard += ProguardMerge.last("net/jpountz/.".r)
proguardMergeStrategies in Proguard += ProguardMerge.discard("META-INF/.".r)
proguardMergeStrategies in Proguard += ProguardMerge.discard("git.properties")
proguardMergeStrategies in Proguard += ProguardMerge.discard("rootdoc.txt")
proguardMergeStrategies in Proguard += ProguardMerge.discard("ght/mi/twm/.".r)
proguardMergeStrategies in Proguard += ProguardMerge.discard("ght/mi/cht/.".r)
proguardMergeStrategies in Proguard += ProguardMerge.discard("ght/mi/imx/.".r)
proguardMergeStrategies in Proguard += ProguardMerge.discard("ght/mi/grpc/.".r)
proguardMergeStrategies in Proguard += ProguardMerge.discard("ght/mi/model/proto/.".r)
proguardMergeStrategies in Proguard += ProguardMerge.discard("NOTICE")
proguardMergeStrategies in Proguard += ProguardMerge.rename("LICENSE.*".r)
```




