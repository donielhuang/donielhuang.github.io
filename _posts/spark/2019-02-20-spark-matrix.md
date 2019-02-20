---
layout: post
comments: false
categories: spark
---

### spark matrix 計算 cosine Similarity

假設有一群人的資料，有每個人的 label 跟分數．將每個人的 label 與分數轉成向量後計算彼此的 cosine Similarity，透過 cosine Similarity 來看這些人的相似程度如何．  

```
val personDatas = Seq(
  ("person1","1:0.5,2:0.3,3:0.4") ,
  ("person2","2:0.7") ,
  ("person3","1:0.9,3:0.1") ,
  ("person4","1:0.3,2:0.6,3:0.8")
)
```
person1 的 label 分數轉成 [0.5,0.3,0.4] 代表一個維度為 3 的向量．  
person2 的 label 分數轉成 [0.0,0.7,0.0]．  
然後用 cosine Similarity 的公式來計算 [0.5,0.3,0.4] 與 [0.0,0.7,0.0] 的 cosine Similarity value．  

接著將上面的資料轉成 IndexedRowMatrix．透過 RDD 的 zipWithIndex，可以取得每個元素的 index 從 0 開始 :   

```
val comparePersons = spark.sparkContext.parallelize(personDatas).toDF("id","labels").cache()

val allPerson = comparePersons.rdd.zipWithIndex.map {
  case (row , index) => {
    val id = row.getAs[String]("id")
    val labels = row.getAs[String]("labels").split(",")
    val lindexs = labels.map(lstr => (lstr.split(":")(0).toInt - 1))
    val lvalues = labels.map(lstr => lstr.split(":")(1).toDouble)
    val labelVector =  org.apache.spark.mllib.linalg.Vectors.sparse(4, lindexs, lvalues)
    (id , new IndexedRow(index , labelVector) )
  }
}.cache()

val indexRowMatrix = new IndexedRowMatrix(allPerson.map(_._2))

```
將 IndexedRowMatrix 轉成 CoordinateMatrix 後轉置(transpose)，然後再轉成 IndexedRowMatrix，
利用 IndexedRowMatrix 的 columnSimilarities 來幫忙算出每個向量之間的相似度 (cosine similarity)．

```
val newMatrix = indexRowMatrix.toCoordinateMatrix.transpose.toIndexedRowMatrix()
val newCosValues = newMatrix.columnSimilarities()
newMatrix.rows.foreach(println(_))
```
印出的結果會是  
```
MatrixEntry(0,1,0.42426406871192845)
MatrixEntry(0,2,0.7652514332541697)
MatrixEntry(0,3,0.8804710999221752)
MatrixEntry(1,3,0.5746957711326908)
MatrixEntry(2,3,0.37020976437050546)

```

### columnSimilarities 使用說明   
1.原來的矩陣  
[0.5 , 0.3 , 0.4 , 0]  
[0 , 0.7 , 0 , 0]  
[0.9 , 0 , 0.1 , 0]  
如果沒轉置使用 columnSimilarities 的話，結果會是  
[0.5 , 0 , 0.9] 跟 [0.3 , 0.7 , 0] 的相似度 0.19130412280981776  
[0.5 , 0 , 0.9] 跟 [0.4 , 0 , 0.1] 的相似度 0.6831571287757409  
[0.5 , 0 , 0.9] 跟 [0 , 0 , 0] 的相似度 NaN (無法計算不顯示)  
[0.3 , 0.7 , 0] 跟 [0.4 , 0 , 0.1] 的相似度 0.3821578531790892  
[0.3 , 0.7 , 0] 跟 [0 , 0 , 0] 的相似度 NaN (無法計算不顯示)  
[0.4 , 0 , 0.1] 跟 [0 , 0 , 0] 的相似度 NaN (無法計算不顯示)
這樣並不是正確的結果，因為希望的是上面三個向量彼此的相似度．所以要將矩陣轉置．

2.轉置後的矩陣  
IndexedRow(0,[0.5,0.0,0.9])  
IndexedRow(1,[0.3,0.7,0.0])  
IndexedRow(2,[0.4,0.0,0.1])  
IndexedRow(3,[0.0,0.0,0.0])  
使用 columnSimilarities 的話，結果會是  
[0.5 , 0.3 , 0.4 , 0] 跟 [0 , 0.7 , 0 , 0] 的相似度 0.42426406871192845  
[0.5 , 0.3 , 0.4 , 0] 跟 [0.9 , 0 , 0.1 , 0] 的相似度 0.7652514332541697  
[0 , 0.7 , 0 , 0] 跟  [0.9 , 0 , 0.1 , 0] 的相似度 0 (相似度 0 的話就不顯示)  
可以用下列的 cosineSimilarityVerifyTest 來驗證相似度是否正確．計算兩個向量的 cosine Similarity，越大代表越像．

```
test("cosineSimilarityVerifyTest") {
	//[0.5 , 0 , 0.9] 跟 [0 , 0 , 0] 的相似度 0
	val query = List[Double](0.5 , 0.3 , 0.4 , 0)
	val labels = List[Double](0 , 0.7 , 0 , 0)
	val cv1 = cosineSimilarity(query.toArray , labels.toArray)
	println("cv1 : " + cv1) // cv1 : 0.42426406871192845
}

def cosineSimilarity(x: Array[Double], y: Array[Double]): Double = {
require(x.size == y.size)
genDot(x, y)/(magnitude(x) * magnitude(y))
}

def genDot(x: Array[Double], y: Array[Double]): Double = {
(for((a, b) <- x.zip(y)) yield a * b).sum
}

def magnitude(x: Array[Double]): Double = {
math.sqrt(x.map(i => i*i).sum)
}
```

### 其他參考作法

```
val spark = SparkSession.builder()
  .master("local[*]")
  .appName("testtest")
  .getOrCreate()

import spark.implicits._

val testSeq = Seq(
  ("1","1:0.5,2:0.3,3:0.4") ,
  ("2","2:0.7") ,
  ("3","1:0.9,3:0.1")
)
val rddEntrys = testSeq.map {
  case(i , labels) => {
    val entrys = labels.split(",").map(l => {
      val index = l.split(":")(0).toInt - 1
      val v = l.split(":")(1).toDouble
      new MatrixEntry(index , (i.toLong - 1) , v )
    })
    entrys
  }
}.flatten

val temp = spark.sparkContext.parallelize(rddEntrys)

val corMatrix = new CoordinateMatrix(temp)
corMatrix.entries.foreach(println(_))

val cv = corMatrix.toIndexedRowMatrix().columnSimilarities()
cv.entries.foreach(println(_))
```
印出結果  
```
MatrixEntry(0,1,0.42426406871192845)
MatrixEntry(0,2,0.7652514332541697)
```

```
val spark = SparkSession.builder()
  .master("local[*]")
  .appName("testtest")
  .getOrCreate()

import spark.implicits._

val testSeq = Seq(
  ("1","1:0.5,2:0.3,3:0.4") ,
  ("2","2:0.7") ,
  ("3","1:0.9,3:0.1")
)
val rddEntrys = testSeq.map {
  case(i , labels) => {
    val entrys = labels.split(",").map(l => {
      val index = l.split(":")(0).toInt - 1
      val v = l.split(":")(1).toDouble
      (index , ((i.toLong - 1).toInt , v) )
    })
    entrys
  }
}.flatten

val indexedRows = spark.sparkContext.parallelize(rddEntrys).groupByKey.map {
  case(i, vectorEntries) => {
    IndexedRow(i, Vectors.sparse(3, vectorEntries.toSeq))
  }
}
val numRows = indexedRows.count

val cv = new IndexedRowMatrix(indexedRows, numRows, 3).columnSimilarities()
cv.entries.foreach(println(_))
```
印出結果  
```
MatrixEntry(0,1,0.42426406871192845)
MatrixEntry(0,2,0.7652514332541697)
```


