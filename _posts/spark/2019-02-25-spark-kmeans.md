---
layout: post
comments: false
categories: spark
---

### spark k-means

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



