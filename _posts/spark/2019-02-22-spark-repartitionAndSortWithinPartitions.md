---
layout: post
comments: false
categories: spark
---

### spark RDD repartitionAndSortWithinPartitions 測試

透過 RDD 的 repartitionAndSortWithinPartitions 可以把 RDD 重新 repartition 並在新的 partition 進行排序．

```
val spark = SparkSession.builder().master("local[*]").appName("testtest").getOrCreate()
val data = spark.sparkContext.parallelize(Seq(5,2,1,66,3,21,52,35,10,88,7,28))
println("bdfore : " + data.getNumPartitions)
val partitionData = data.zipWithIndex.repartitionAndSortWithinPartitions(new HashPartitioner(3))
println("after : " + partitionData.getNumPartitions)
val accum = spark.sparkContext.longAccumulator("My Accumulator")
partitionData.mapPartitions(it => {
  val topData = it.toSeq.iterator
  val dataStr = topData.mkString(";")
  dataStr.split(";").foreach(d => accum.add(d.split(",")(0).replace("(","").toInt))
  println(accum.value + " # " + dataStr)
  topData
}).collect()
println("accum value is " + accum.value)
```

根據下列的結果可以看出，原來的 partition 是 8，repartition 後就變成給定的 3，partition 內容 :  
(2,1);(5,0);(35,7) 在同一個 partition  
(3,4);(21,5);(66,3) 在同一個 partition  
(1,2);(7,10);(10,8);(28,11);(52,6);(88,9) 在同一個 partition  
透過 spaark 的 Accumulator 可以看出每個 partition 的狀況，以及最後算出來的結果．  

```
bdfore : 8
after : 3
42 # (2,1);(5,0);(35,7)
90 # (3,4);(21,5);(66,3)
186 # (1,2);(7,10);(10,8);(28,11);(52,6);(88,9)
accum value is 318
```

接著只要修改這一行，就可以達到 top N 的 效果．

```
val topData = it.toSeq.take(2).iterator
```
就會取各自 partition 的前 2 個 element．  

```
7 # (2,1);(5,0)
24 # (3,4);(21,5)
8 # (1,2);(7,10)
accum value is 39
```
如果是要針對所有的元素排序可以把 HashPartitioner 的值設 1，表示所有的元素都會在同一個 partition 裡．

```
val partitionData = data.zipWithIndex.repartitionAndSortWithinPartitions(new HashPartitioner(1))
```
結果會是  

```
bdfore : 8
after : 1
3 # (1,2);(2,1)
accum value is 3
```







