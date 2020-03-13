---
layout: post
comments: false
categories: spark
---

## 動機
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

再根據 id group by 把資料整成一筆，結果出現了 data type mismatch 錯誤訊息
```
scala> val df4 = df3.withColumn("locations", array(col("latlng"),col("conf"),col("visits")))
org.apache.spark.sql.AnalysisException: cannot resolve 'array(`latlng`, `conf`, `visits`)' due to data type mismatch: input to function array should all be the same type, but it's [array<double>, bigint, array<array<bigint>>];;
```
所以改用 struct 資料型態，struct 解釋 (StructType objects define the schema of Spark DataFrames. StructType objects contain a list of StructField objects that define the name, type, and nullable flag for each column in a DataFrame.)
```
val df4 = df3.withColumn("locations", struct(col("latlng"),col("conf"),col("visits")))
```
再用 id group 起來變成一筆
```
val df5 = df4.groupBy("id").agg(collect_list("locations"))
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
