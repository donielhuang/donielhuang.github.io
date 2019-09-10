---
layout: post
comments: false
categories: spark
---

### spark cassandra spanByKey

原本有一段程式是從 cassandra 把資料查出來，轉成 person 物件後，再根據 person id 當作 key，然後 reduceByKey 後做 Person merge．

```
val persons = {
  sc.cassandraTable[PersonByTime](cassandraKeyspace, cassandraTableName).keyBy[(String, String, Long)]("groupid","id","inserttime").map(p => {
    (p._1._2 , PersonParser.modelString(p._2.personString))
  }).reduceByKey {
    Person.merge
  }.filter(p => !"".equals(p._1)).coalesce(500)
}
```

執行的結果 shuffle write 數量非常高，有可能是因為 reduceByKey 的關係．

![sparkCassandraSpanbykey_1.jpg](/static/img/spark/sparkCassandraSpanbykey/sparkCassandraSpanbykey_1.jpg){:height="400px" width="800px"}


於是使用 spark cassandra connector 提供的 spanByKey 根據 groupid 以及 id 來轉換成 spark rdd，等於說根據這樣的 partition 該 partition 的資料已經都是要 merge 的資料了．
所以不需要再 reduceByKey 只要把 partition 的 person 全部都 merge 起來．

```
val persons = {
  sc.cassandraTable[PersonByTime](cassandraKeyspace, cassandraTableName).keyBy[(String, String)]("groupid","id").spanByKey.coalesce(500).mapPartitions(
   iter => {
      for(p <- iter) yield {
        val groupKey = p._1._1
        val personKey = p._1._2
        val person = p._2.foldLeft(new Person(groupid = groupKey,id = personKey)) {
          case (p , ps) => {
            Person.merge(p , PersonParser.modelString(ps.personString))
          }
        }
        (personKey , person)
      }
    }
  )
}
```

這樣修改後 shuffle write 從 84.4 G 變成 3.1 M．


官網解釋 [spanByKey](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/3_selection.md) : 
```
The methods spanBy and spanByKey iterate every Spark partition locally and put every RDD item into the same group as long as the key doesn't change.
```