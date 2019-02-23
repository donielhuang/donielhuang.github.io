---
layout: post
comments: false
categories: spark
---

### spark query Cassandra

建立 Cassandra KEYSPACE 以及 TABLE
```
CREATE KEYSPACE miks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' }  AND durable_writes = true;

CREATE TABLE miks2.testpersonlabelvector(
id text,
labelvector list<double>,
PRIMARY KEY(id)
) ;
```

看 KEYSPACE 狀態

```
nodetool status miks2 ;
```

使用 spark Cassandra connector 對 cassandra 做查詢，詳細資料可以參考[spark-cassandra-connector](https://github.com/datastax/spark-cassandra-connector)．

```
import com.datastax.spark.connector._

val spark = SparkSession.builder()
  .appName("LookLikePersonLSH")
  .master("local[*]")
  .config("spark.cassandra.connection.host", "192.168.6.31,192.168.6.32,192.168.6.33")
  .config("spark.cassandra.auth.username", "miuser")
  .config("spark.cassandra.auth.password", "mimimi123")
  .getOrCreate()

val allPersons = spark.sparkContext.cassandraTable[(String,Seq[Double])]("miks2","testpersonlabelvector")
      .select("id","labelvector").keyBy[Tuple1[String]]("id")

val testpersonSeed = "hash:52f479d0-126c-4ccd-bf3b-3d99a6d3fec0,hash:79bdf00a-b739-42a5-bb3c-c55315011b52,hash:08565f43-5e61-4d4f-ac5c-d2fa2416e8f0"
val seedPersons = spark.sparkContext.cassandraTable[(String,Seq[Double])]("miks2","testpersonlabelvector")
      .select("id","labelvector").where("id in ? " , testpersonSeed.split(",").toSeq).keyBy[Tuple1[String]]("id")
```

計算結果並存到 HDFS

```
val tempList = seedPersons.cartesian(allPersons).map(ds => (ds._1._1._1 , (ds._2._1._1 , LabelVectorUtil().cosineSimilarity(ds._1._2._2 , ds._2._2._2))))
val takeCount = (outputPersons / allPersons.getNumPartitions)
val resultIdList = tempList.mapPartitions(rowit => {
    val tempDatas = rowit.toSeq.groupBy(_._1)
    val takeCnt = (takeCount / tempDatas.size) + 1
    tempDatas.map(personInfos => {
      personInfos._2.sortWith(_._2._2 > _._2._2).take(takeCnt)
    }).flatten.toIterator
  }).cache()
resultIdList.saveAsTextFile("/enrich/tempResult/LookLikePersons")
```








