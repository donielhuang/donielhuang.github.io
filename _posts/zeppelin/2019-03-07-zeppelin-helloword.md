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







