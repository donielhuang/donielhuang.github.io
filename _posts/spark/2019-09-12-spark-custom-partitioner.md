---
layout: post
comments: false
categories: spark
---

### custom spark Partitioner
測試環境 hadoop cluster :
```
3 台 vm :
memory total : 138 GB
core total : 30
```
所以 partition 數最好控制在 3 或 4 倍的話是，90 ~ 40 之間


測試約 14.4G 的資料量．

```
> hdfs dfs -du -s -h /data/pool/stresstest/*
279.0 K  /data/pool/stresstest/LSR_20190608.csv
360.7 M  /data/pool/stresstest/LSR_20190609.csv
4.4 G  /data/pool/stresstest/LSR_20190610.csv
901.8 M  /data/pool/stresstest/LSR_20190611.csv
4.4 G  /data/pool/stresstest/LSR_20190701.csv
4.4 G  /data/pool/stresstest/LSR_20190801.csv
```

LocationUpdate.scala

```
package ght.mi.twm.poc

import com.datastax.spark.connector._
import ght.mi.core.cassandra.model.Person
import ght.mi.core.mapping.config.{DataType, EnrichConfig}
import ght.mi.twm.config.UpdateSetting
import ght.mi.twm.tool.{JsonInterface, PersonParser}
import org.apache.spark.sql.SparkSession
/**
  *
  *
  * spark-submit --class ght.mi.twm.poc.LocationUpdate --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 10g --executor-cores 2 --num-executors 6 enrich-5.0.3.jar /data/pool/stresstest/LSR* /data/pool/stresstest/dailyPerson
  *
  * /data/pool/daily/20190608
  *
  * */
object LocationUpdate {

  def main(args: Array[String]): Unit = {

    val locationPath = args(0)
    val dailyPersonOutputPath = args(1)

    val spark: SparkSession = SparkSession.builder()
      .config("spark.sql.warehouse.dir", "/tmp")
      .config("spark.cassandra.connection.host", EnrichConfig.CASSANDRA_IPS)
      .config("spark.cassandra.auth.username", EnrichConfig.CASSANDRA_USER)
      .config("spark.cassandra.auth.password", EnrichConfig.CASSANDRA_PSWD)
      .getOrCreate()

    val sc = spark.sparkContext

    val setting = new UpdateSetting(
      dataType = DataType.LOCATION ,
      labelCritDir = EnrichConfig.LABEL_CRIT_DIR
    )

    val jsonFile = sc.textFile(setting.labelCritDir).collect.mkString.replace(" ", "")
    val profileSink = sc.broadcast(JsonInterface.json2ProfileCritUnits(JsonInterface.readJSON(jsonFile)))

    val repartitionLocationRdd = sc.textFile(locationPath).flatMap(
      str => {
        PersonParser.stringToPerson(str,DataType.LOCATION ,profileSink)
      }
    )

    val locationRdd = repartitionLocationRdd.reduceByKey{
      Person.merge
    }.map(_._2.toString).cache()
    
    locationRdd.saveAsTextFile(dailyPersonOutputPath)


    val totalInsertPerson = sc.textFile(dailyPersonOutputPath).map(
      str => {
        val p = PersonParser.modelString(str)
        p.id -> p
      }
    ).reduceByKey {
      Person.merge
    }.map(p => {
      (p._2.groupid , p._2.id , (System.currentTimeMillis()/1000) , p._2.toString)
    }).cache()

    totalInsertPerson.saveToCassandra(EnrichConfig.CASSANDRA_KEYSPACE , "person_by_string_time")
  }

}

```
執行結果約 27 min

![sparkPartitionBy_1.jpg](/static/img/spark/sparkPartitionBy/sparkPartitionBy_1.jpg){:height="400px" width="800px"}



客製 TwmPersonPartitioner : 

```
package ght.mi.twm.job

import org.apache.spark.Partitioner

class TwmPersonPartitioner(partitions:Int) extends Partitioner {

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {

    val personId = key.asInstanceOf[String].replace("hash:" , "")

    //0~99 2位數
    val groupId = if("".equals(personId.trim) || personId == null) {
      "0"
    } else if(personId.length < 3) {
      personId
    } else {
      personId.substring(personId.length - 2)
    }

    /*
    // 0~9999 4位數
    val groupId = if("".equals(personId.trim) || personId == null) {
      "0"
    } else if(personId.length < 5) {
      personId
    } else {
      personId.substring(personId.length - 4)
    }
    */

    /*
    //0~999999 6位數
    val groupId = if("".equals(personId.trim) || personId == null) {
      "0"
    } else if(personId.length < 7) {
      personId
    } else {
      personId.substring(personId.length - 6)
    }
    */

    groupId.toInt
  }
}

```

在使用 partitionBy 分 partition 100 個
```
val repartitionLocationRdd = sc.textFile(locationPath).flatMap(
  str => {
    PersonParser.stringToPerson(str,DataType.LOCATION ,profileSink)
  }
).partitionBy(new TwmPersonPartitioner(100)) // 這邊的數量要與 getPartition 相同否則超過時會出現 java.lang.ArrayIndexOutOfBoundsException 
```


修改使用 spanByKey 效能有變快但結果不確定是否正確，因為 output size 變大了，要在驗證結果．


```
package ght.mi.twm.poc

import com.datastax.spark.connector._
import ght.mi.core.cassandra.model.Person
import ght.mi.core.mapping.config.{DataType, EnrichConfig}
import ght.mi.twm.config.UpdateSetting
import ght.mi.twm.job.TwmPersonPartitioner
import ght.mi.twm.tool.{JsonInterface, PersonParser}
import org.apache.spark.sql.SparkSession
/**
  *
  *
  * spark-submit --class ght.mi.twm.poc.LocationUpdate --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 15g --executor-cores 4 --num-executors 12 enrich-5.0.3.jar /data/pool/stresstest/LSR* /data/pool/stresstest/dailyPerson
  *
  * /data/pool/daily/20190608
  *
  * */
object LocationUpdate {

  def main(args: Array[String]): Unit = {

    val locationPath = args(0)
    val dailyPersonOutputPath = args(1)

    val spark: SparkSession = SparkSession.builder()
      .config("spark.sql.warehouse.dir", "/tmp")
      .config("spark.cassandra.connection.host", EnrichConfig.CASSANDRA_IPS)
      .config("spark.cassandra.auth.username", EnrichConfig.CASSANDRA_USER)
      .config("spark.cassandra.auth.password", EnrichConfig.CASSANDRA_PSWD)
      .getOrCreate()

    val sc = spark.sparkContext

    val setting = new UpdateSetting(
      dataType = DataType.LOCATION ,
      labelCritDir = EnrichConfig.LABEL_CRIT_DIR
    )

    val jsonFile = sc.textFile(setting.labelCritDir).collect.mkString.replace(" ", "")
    val profileSink = sc.broadcast(JsonInterface.json2ProfileCritUnits(JsonInterface.readJSON(jsonFile)))

    val repartitionLocationRdd = sc.textFile(locationPath).flatMap(
      str => {
        PersonParser.stringToPerson(str,DataType.LOCATION ,profileSink)
      }
    )
    repartitionLocationRdd.spanByKey.mapPartitions(
      iter =>
        for(p <- iter) yield {
          val personKey = p._1
          val person = p._2.foldLeft(new Person(id = personKey)) {
            case (p , ps) => {
              Person.merge(p , ps)
            }
          }
          person.toString
        }
    ).saveAsTextFile(dailyPersonOutputPath)

    /*
    val locationRdd = repartitionLocationRdd.reduceByKey{
      Person.merge
    }.map(_._2.toString).cache()

    locationRdd.saveAsTextFile(dailyPersonOutputPath)
    */


    val totalInsertPerson = sc.textFile(dailyPersonOutputPath).map(
      str => {
        val p = PersonParser.modelString(str)
        p.id -> p
      }
    ).reduceByKey {
      Person.merge
    }.map(p => {
      (p._2.groupid , p._2.id , (System.currentTimeMillis()/1000) , p._2.toString)
    }).cache()

    totalInsertPerson.saveToCassandra(EnrichConfig.CASSANDRA_KEYSPACE , "person_by_string_time")
  }

}

```

![sparkPartitionBy_2.jpg](/static/img/spark/sparkPartitionBy/sparkPartitionBy_2.jpg){:height="400px" width="800px"}




















