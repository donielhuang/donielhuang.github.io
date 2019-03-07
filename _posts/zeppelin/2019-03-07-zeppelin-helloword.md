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



