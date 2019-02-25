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

### 根據 RDD 的 Key 做 partition

假設有下列資料，希望根據資料的 key 來分成各自的 spark partition．

```
val datas = Seq(
  (1,Seq((1,2),(1,1))) ,
  (2,Seq((2,3),(2,2))) ,
  (3,Seq((4,5),(3,3))) ,
  (1,Seq((6,7),(4,4)))
)
```

先來看透過 spark parallelize 會怎麼分 partition．

```
val drdd = spark.sparkContext.parallelize(datas)
println("a-> " + drdd.getNumPartitions)
val taccum = spark.sparkContext.longAccumulator("My Accumulator")
drdd.foreachPartition(it => {
  it.foreach(t => {
    taccum.add(t._1)
    println(taccum.value + " ; " + t._2.mkString(","))
  })
})
```
總共會分成 8 個 partition，但並不是我們要的．

```
a-> 8
1 ; (6,7),(4,4)
1 ; (1,2),(1,1)
2 ; (2,3),(2,2)
3 ; (4,5),(3,3)
```

所以透過 groupByKey 再 partitionBy 根據 key 的人數重新 partition．  

```
val keycount = drdd.groupByKey.count().toInt
val repartitionRdd = drdd.groupByKey.partitionBy(new HashPartitioner(keycount))
```


```
println("keycount : " + keycount)
println("b-> " + repartitionRdd.getNumPartitions)
val accum = spark.sparkContext.longAccumulator("My Accumulator")
repartitionRdd.foreachPartition(it => {
  it.foreach(t => {
    accum.add(t._1)
    println(accum.value + " ; " + t._2.mkString(","))
  })
})
```
結果會是  

```
keycount : 3
b-> 3
1 ; List((1,2), (1,1)),List((6,7), (4,4))
2 ; List((2,3), (2,2))
3 ; List((4,5), (3,3))
```






