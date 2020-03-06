---
layout: post
comments: false
categories: spark
---


當在處理 json、parquet、Avro 或 xml 時常會有一些資料類型是 arrays、lists 或 maps 之類的，這時候可以透過 explode 將這些 collection 欄位反正規化轉成一個欄位增加處理的效能．

students.json

```json
{"name":"Michael", "age":25,"myScore":[{"score1":19,"score2":23},{"score1":58,"score2":50}]}
{"name":"Andy", "age":30,"myScore":[{"score1":29,"score2":33},{"score1":38,"score2":52,"score3":60},{"score1":88,"score2":71}]}
{"name":"Justin", "age":19,"myScore":[{"score1":39,"score2":43},{"score1":28,"score2":53}]}
```

先讀取 json file，可以看到 myScore 是一個 array 裡面存放多個型態是 struct 的 element．

```scala
scala> val df1 = spark.read.json("/temp/students.json")
df1: org.apache.spark.sql.DataFrame = [age: bigint, myScore: array<struct<score1:bigint,score2:bigint,score3:bigint>> ... 1 more field]

scala> df1.printSchema()
root
 |-- age: long (nullable = true)
 |-- myScore: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- score1: long (nullable = true)
 |    |    |-- score2: long (nullable = true)
 |    |    |-- score3: long (nullable = true)
 |-- name: string (nullable = true)

scala> df1.show()
+---+--------------------+-------+
|age|             myScore|   name|
+---+--------------------+-------+
| 25|[[19, 23,], [58, ...|Michael|
| 30|[[29, 33,], [38, ...|   Andy|
| 19|[[39, 43,], [28, ...| Justin|
+---+--------------------+-------+
```

透過 explode 把 myScore 欄位反正規化

```scala
scala> val df2 = df1.select(df1("name"),df1("age"),explode(df1("myScore"))).toDF("name","age","myScore")
df2: org.apache.spark.sql.DataFrame = [name: string, age: bigint ... 1 more field]

scala> df2.printSchema()
root
 |-- name: string (nullable = true)
 |-- age: long (nullable = true)
 |-- myScore: struct (nullable = true)
 |    |-- score1: long (nullable = true)
 |    |-- score2: long (nullable = true)
 |    |-- score3: long (nullable = true)

scala> df2.show()
+-------+---+------------+
|   name|age|     myScore|
+-------+---+------------+
|Michael| 25|   [19, 23,]|
|Michael| 25|   [58, 50,]|
|   Andy| 30|   [29, 33,]|
|   Andy| 30|[38, 52, 60]|
|   Andy| 30|   [88, 71,]|
| Justin| 19|   [39, 43,]|
| Justin| 19|   [28, 53,]|
+-------+---+------------+
```

再取出各自的欄位

```scala
scala> val df3 = df2.select("name","age","myScore.score1","myScore.score2","myScore.score3")
df3: org.apache.spark.sql.DataFrame = [name: string, age: bigint ... 3 more fields]

scala> df3.show()
+-------+---+------+------+------+
|   name|age|score1|score2|score3|
+-------+---+------+------+------+
|Michael| 25|    19|    23|  null|
|Michael| 25|    58|    50|  null|
|   Andy| 30|    29|    33|  null|
|   Andy| 30|    38|    52|    60|
|   Andy| 30|    88|    71|  null|
| Justin| 19|    39|    43|  null|
| Justin| 19|    28|    53|  null|
+-------+---+------+------+------+
```

使用 withColumn 可以多加欄位資料

```scala
scala> df2.withColumn("myScore2", df2("myScore")).show()
+-------+---+------------+------------+
|   name|age|     myScore|    myScore2|
+-------+---+------------+------------+
|Michael| 25|   [19, 23,]|   [19, 23,]|
|Michael| 25|   [58, 50,]|   [58, 50,]|
|   Andy| 30|   [29, 33,]|   [29, 33,]|
|   Andy| 30|[38, 52, 60]|[38, 52, 60]|
|   Andy| 30|   [88, 71,]|   [88, 71,]|
| Justin| 19|   [39, 43,]|   [39, 43,]|
| Justin| 19|   [28, 53,]|   [28, 53,]|
+-------+---+------------+------------+
```

如果想要再把 score1、score2、score3 反正規劃成一個 score 欄位可以透過 array 這個 function，先把這三個欄位合併成一個 scores 欄位然後再透過 explode 展開一次即可．

```scala
scala> df3.withColumn("scores", array(df3("score1"),df3("score2"),df3("score3"))).select($"name",$"age",explode($"scores")).show()
+-------+---+----+
|   name|age| col|
+-------+---+----+
|Michael| 25|  19|
|Michael| 25|  23|
|Michael| 25|null|
|Michael| 25|  58|
|Michael| 25|  50|
|Michael| 25|null|
|   Andy| 30|  29|
|   Andy| 30|  33|
|   Andy| 30|null|
|   Andy| 30|  38|
|   Andy| 30|  52|
|   Andy| 30|  60|
|   Andy| 30|  88|
|   Andy| 30|  71|
|   Andy| 30|null|
| Justin| 19|  39|
| Justin| 19|  43|
| Justin| 19|null|
| Justin| 19|  28|
| Justin| 19|  53|
+-------+---+----+
only showing top 20 rows
```






