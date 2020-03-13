---
layout: post
comments: false
categories: spark
---

需要將某段 spark 程式本來使用 RDD 操作，改成用 Dataframe 的方式，所以需對 Dataframe 新增或組合欄位，把整段過程整理下來．

1. 首先先啟動一個 spark-shell，在 shell 裡面做操作
```
spark-shell --master yarn --deploy-mode client --executor-memory 20g --executor-cores 6 --num-executors 3
```

2.將 parquet file 讀進來
```
val df = spark.read.parquet("/tmp/kelvin/pool/20200306/location")
```
parquet file 資料格式如下
```
> df.printSchema()
root
 |-- id: string (nullable = true)
 |-- lat: double (nullable = true)
 |-- lng: double (nullable = true)
 |-- startTime: long (nullable = true)
 |-- endTime: long (nullable = true)
 |-- conf: long (nullable = true)
 |-- tags: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
```

如果想看某比 id 的筆數可以透過 where 再 count
```
df.where($"id" === "1000567956").count()
```

3. 接著目標把 id,lat,lng group 起來看減少資料的筆數

新增 duration 欄位，利用 when 和 otherwise 搭配將 null 的值轉為 0，再用 endTime - startTime
```
val df1 = df.withColumn("duration", when(col("endTime").isNull, lit(0)).otherwise(col("endTime")) - when(col("startTime").isNull, lit(0)).otherwise(col("startTime")))
```

再把 startTime 跟 duration 組成一個 array，lat 和 lng 組成 array
```
val df2 = df1.withColumn("visits", array(col("startTime"),col("duration"))).withColumn("latlng", array(col("lat"),col("lng"))).select("id","latlng","conf","visits")
```

將 id 和 latlng groupBy 起來，然後 aggreate conf 取最小值，visits 組成 list[array]
```
val df3 = df2.groupBy("id","latlng").agg(min("conf"),collect_list($"visits"))
```
如果要幫 aggreate 欄位命名可以再後面加上 .as() 要的名稱否則名稱為是 `min("conf")`
```
scala> val df3 = df2.groupBy("id","latlng").agg(min("conf").as("conf"),collect_list($"visits").as("visits"))
df3: org.apache.spark.sql.DataFrame = [id: string, latlng: array<double> ... 2 more fields]
```
看整理好的 schema
```
scala> df3.printSchema()
root
 |-- id: string (nullable = true)
 |-- latlng: array (nullable = false)
 |    |-- element: double (containsNull = true)
 |-- conf: long (nullable = true)
 |-- visits: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: long (containsNull = true)
```
資料格式
```
scala> df3.show(3,false)
+----------+-----------------------------------------+----+----------------------+
|id        |latlng                                   |conf|visits                |
+----------+-----------------------------------------+----+----------------------+
|1000567956|[-89.8275584137195, -87.8337667722156]   |324 |[[-1139906190, 67428]]|
|1000567956|[-89.7455462489989, 168.18234956471753]  |141 |[[1516339667, 31048]] |
|1000567956|[-89.67407963954729, -128.94006130577958]|346 |[[1237416763, 47404]] |
+----------+-----------------------------------------+----+----------------------+
only showing top 3 rows
```

再根據 id group by 把資料整成一筆，結果出現了 data type mismatch 錯誤訊息
```
scala> val df4 = df3.withColumn("locations", array(col("latlng"),col("conf"),col("visits")))
org.apache.spark.sql.AnalysisException: cannot resolve 'array(`latlng`, `conf`, `visits`)' due to data type mismatch: input to function array should all be the same type, but it's [array<double>, bigint, array<array<bigint>>];;
```
所以改用 struct 資料型態，struct 解釋 (StructType objects define the schema of Spark DataFrames. StructType objects contain a list of StructField objects that define the name, type, and nullable flag for each column in a DataFrame.)
```
val df4 = df3.withColumn("locations", struct(col("latlng"),col("conf"),col("visits")))
```
資料格式如下
```
scala> df4.show(3,false)
+----------+-----------------------------------------+----+----------------------+-----------------------------------------------------------------------+
|id        |latlng                                   |conf|visits                |locations                                                              |
+----------+-----------------------------------------+----+----------------------+-----------------------------------------------------------------------+
|1000567956|[-89.8275584137195, -87.8337667722156]   |324 |[[-1139906190, 67428]]|[[-89.8275584137195, -87.8337667722156], 324, [[-1139906190, 67428]]]  |
|1000567956|[-89.7455462489989, 168.18234956471753]  |141 |[[1516339667, 31048]] |[[-89.7455462489989, 168.18234956471753], 141, [[1516339667, 31048]]]  |
|1000567956|[-89.67407963954729, -128.94006130577958]|346 |[[1237416763, 47404]] |[[-89.67407963954729, -128.94006130577958], 346, [[1237416763, 47404]]]|
+----------+-----------------------------------------+----+----------------------+-----------------------------------------------------------------------+
only showing top 3 rows
```

再用 id group 起來變成一筆
```
val df5 = df4.groupBy("id").agg(collect_list("locations"))
```







#### flatten
透過 flatten 可以把 aggreate 的 list 展開來．
```
import org.apache.spark.sql.functions.flatten
val trackingVisits = flatten(collect_list($"visits")).alias("visitsList")

val trackingvisitsList = flatten(collect_list($"visitsList")).alias("visitsListList")
val trackinglatlng = flatten(collect_list($"latlng")).alias("latlngList")

val df5 = df4.groupBy("id","lat","lng").agg(trackingVisits)

val df6 = df5.withColumn("latlng", array(df5("lat"),df5("lng")))

val df7 = df6.groupBy("id").agg(trackingvisitsList,trackinglatlng)
```








Sample Code
```scala
package ght.mi.cht.job.cassandra

import org.apache.spark.sql.{Row, SparkSession}

object UpdateLocByLocal {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "/tmp").master("local[6]").getOrCreate()
    //import spark.implicits._
    import org.apache.spark.sql.types._

    val locSchema = StructType(
      Array(
        StructField("hashid", StringType, true),
        StructField("make", StringType, true),
        StructField("model", StringType, true),
        StructField("startTime", StringType, true),
        StructField("endTime", StringType, true),
        StructField("lat", DoubleType, true),
        StructField("lng", DoubleType, true),
        StructField("indoor", StringType, true),
        StructField("confidence", StringType, true)
      )
    )

    import org.apache.spark.sql.functions._

    val df = spark.read.schema(locSchema).csv("file:/Volumes/Transcend/local/testData/PCA/20190901.out")
      .withColumn("startTimeLong",to_timestamp(col("startTime"),"yyyy-MM-dd HH:mm:ss").cast("long"))
      .withColumn("endTimeLong",to_timestamp(col("endTime"),"yyyy-MM-dd HH:mm:ss").cast("long"))
      .withColumn("duration", when(col("endTimeLong").isNull, lit(0)).otherwise(col("endTimeLong")) - when(col("startTimeLong").isNull, lit(0)).otherwise(col("startTimeLong")))
      .where("duration > 10")
      .select("hashid","lat","lng", "confidence" ,"startTimeLong","duration")
      .withColumn("visits", struct(col("startTimeLong").as("startTime"),col("duration").as("duration")))
      .withColumn("latlng", struct(col("lat"),col("lng")))
      .select("hashid","confidence","latlng","visits")

    val df1 = df.groupBy("hashid","latlng")
      .agg(min("confidence").as("conf"),collect_list("visits").as("visits"))
    //val df2 = df1.withColumn("locations",struct("latlng","conf","visits")).groupBy("hashid").agg(collect_list("locations").as("locations"))
    //df2.show(5,false)
    //df2.printSchema()
    /*
root
 |-- hashid: string (nullable = true)
 |-- collect_list(locations): array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- latlng: struct (nullable = false)
 |    |    |    |-- lat: double (nullable = true)
 |    |    |    |-- lng: double (nullable = true)
 |    |    |-- conf: string (nullable = true)
 |    |    |-- visits: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- startTime: long (nullable = true)
 |    |    |    |    |-- duration: long (nullable = true)
    */
    //df2.write.json("/Volumes/Transcend/temp/json/result")
    /* */
    val df2 = df1.withColumn("locations",struct("latlng","conf","visits")).groupBy("hashid").agg(collect_list("locations").as("locations"),count("locations").as("count"))

    //.where("hashid='62687020'")
    df2.rdd.map(row => {
      val hashId = row.getAs[String]("hashid")
      val locations = row.getList[Row](1)
      // val locations = row.getAs[Row]("locations") // 這樣寫會出現 Caused by: java.lang.ClassCastException: scala.collection.mutable.WrappedArray$ofRef cannot be cast to org.apache.spark.sql.Row
      val locstr = locations.toArray.map(r => {
        val rdata = r.asInstanceOf[Row]
        val latlng = rdata.getAs[Row]("latlng")
        val conf = rdata.getAs[String]("conf")
        val visits = rdata.getList[Row](2)

        val visitsstr = visits.toArray.map(rr => {
          val rrrdata = rr.asInstanceOf[Row]
          val sstime = rrrdata.getAs[Long]("startTime")
          val ssdu = rrrdata.getAs[Long]("duration")
          s"$sstime,$ssdu"
        }).mkString(";")
        s"$latlng,$conf,$visitsstr"
      }).mkString(";")

      /*
      val locationsStr = locations.map(r => {
        val latLng = r.getAs[Row]("latlng")
        val lat = r.getAs[Double]("lat")
        val lng = r.getAs[Double]("lng")
        val conf = r.getAs[String]("conf")
        val visitsArray = r.getAs[Array[Row]]("visits")
        val visitsStr = visitsArray.map(tr => {
          val st = tr.getAs[Double]("startTime")
          val d = tr.getAs[Double]("duration")
          s"$st:$d"
        }).mkString("#")

        s"$lat,$lng,$conf,$visitsStr"
      })
      s"$hashId-$locationsStr"
      */
      s"$hashId-$locstr"
    }).foreach(println)


      //.saveAsTextFile("/Volumes/Transcend/temp/json/result")
  }

}
/*
array of struct
https://sparkbyexamples.com/spark/explode-array-of-struct-to-rows/

加上判斷式 會是 true
val actualDF = df.withColumn(
  "animal_interpretation",
  struct(
    (col("weight") > 5).as("is_large_animal"),
    col("animal_type").isin("rat", "cat", "dog").as("is_mammal")
  )
)
*/
```
output 資料如下
```
62764741-[24.959,121.412],Medium,1567301851,12;1567316211,25;[24.96,121.416],High,1567291717,13;[24.966,121.425],High,1567319275,25;[24.962,121.415],High,1567286987,11;1567286957,27;1567300407,13;1567319022,25;[24.962,121.418],Medium,1567300466,25;[24.962,121.426],Medium,1567275862,27;[24.961,121.415],High,1567271621,40;1567271546,25;1567288414,25;1567300347,55;1567304218,29;1567314738,40;[24.966,121.427],High,1567275892,25;[24.968,121.425],High,1567291375,26;1567291405,28;[24.961,121.418],High,1567292744,25;[24.962,121.414],High,1567320569,25;[24.967,121.426],High,1567352839,25;[24.962,121.421],High,1567291568,12;[24.967,121.425],High,1567320316,40;[24.964,121.416],High,1567353077,29;[24.962,121.416],High,1567320540,24;[24.963,121.427],Medium,1567352795,24;[24.958,121.416],Medium,1567314857,40;[24.962,121.429],Medium,1567320302,12;[24.961,121.412],High,1567294172,40;[24.964,121.426],High,1567352824,11;[24.959,121.413],High,1567272276,14;1567271308,25;1567276175,14;1567276190,11;1567274568,25;1567274613,12;1567276145,11;1567280799,11;1567285129,24;1567285158,25;1567282243,14;1567284021,17;1567282273,25;1567285099,29;1567286378,13;1567287106,11;1567288281,26;1567286392,25;1567287032,26;1567288340,40;1567291732,25;1567294306,28;1567292402,24;1567294544,25;1567292684,14;1567297246,12;1567301866,12;1567301881,11;1567300318,24;1567299975,11;1567304203,11;1567304054,27;1567303235,25;1567306761,11;1567306627,11;1567308057,25;1567307372,55;1567312699,11;1567313741,11;1567313964,11;1567312402,11;1567312327,11;1567313845,11;1567313666,11;1567314038,11;1567314053,11;1567314068,11;1567313994,11;1567314173,40;1567314098,11;1567314113,11;1567318829,40;1567318948,25;1567320629,12;1567323440,25;1567323187,14;1567313875,11;1567313949,11;1567312387,11;1567328846,55;1567353494,24;[24.962,121.422],High,1567320450,13;[24.964,121.414],High,1567353166,25;[24.968,121.431],High,1567352629,12;[24.96,121.419],High,1567305855,14;[24.969,121.434],High,1567305498,11;[24.966,121.429],High,1567320287,13;[24.96,121.411],High,1567276085,44;1567301822,13;1567320599,25;[24.96,121.418],High,1567305914,25;1567305974,25;1567306018,25;1567316092,25;1567316062,25;1567353107,14;[24.965,121.427],High,1567319246,26;[24.961,121.421],High,1567305736,23;[24.961,121.422],High,1567304337,25;[24.964,121.415],High,1567306346,25;[24.965,121.428],High,1567319305,19;[24.963,121.416],High,1567272202,12;1567286422,25;[24.958,121.413],High,1567316835,25;[24.961,121.416],High,1567316151,55;[24.967,121.429],High,1567304456,40;1567305512,25;1567305542,40;1567352765,11;[24.961,121.414],High,1567272232,39;1567305884,25;1567314932,39;[24.963,121.415],High,1567286496,55;[24.966,121.431],High,1567320257,25
62051893-[24.987,121.426],Medium,1567327851,15;1567327833,11;[24.995,121.428],High,1567312536,25;1567310500,22;1567312108,21;1567317558,22;1567318318,22;1567319507,25;1567319000,25;1567322649,25;1567325330,25;1567327713,14;1567327788,40;1567327654,16;1567327744,24;1567327684,26;[25.018,121.408],High,1567294213,22;1567331998,12;[24.998,121.414],High,1567328251,12;[25.013,121.412],High,1567328579,23;[25.016,121.408],High,1567328673,18;[24.995,121.421],High,1567328071,39;1567328118,22;[24.993,121.427],High,1567328043,24;[25.017,121.408],High,1567331273,22;1567330603,11;1567330704,13;1567337004,25;[24.996,121.427],High,1567309113,25;[25.015,121.41],High,1567328606,26;[25.017,121.41],High,1567283572,18;1567285553,18;1567291400,22;[25.017,121.407],High,1567276704,22;1567288809,22;1567294109,22;1567332506,25;[25.028,121.412],Low,1567308905,25;[25.018,121.407],High,1567267202,11;1567298945,25;1567299141,22;1567333983,22;1567333432,22;[25.004,121.416],High,1567328428,31;[25.011,121.412],High,1567328532,29;[25.012,121.412],High,1567308622,41;[25.017,121.412],Medium,1567308802,39;[25.003,121.423],High,1567308846,24
...
```



* 如果有做 a.leftOuterJoin(b) 有出現很多空的 key 時，這些空的節點被分配到同一節點中也有可能導致該節點崩潰．





