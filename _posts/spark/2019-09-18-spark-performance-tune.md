---
layout: post
comments: false
categories: spark
---

## Transformations
Transformations 的操作有分為 Narrow 和 Wide．

#### Narrow Transformations 
表示從 parent RDD 經過 Narrow Transformations 的轉換後，還是只會成為單一的 partition．屬於一對一的關係，所以 partition 的資料不會跨 partition 造成 shuffle．
相關的操作有 map , filter , union．

#### Wide Transformations 
表示從 parent RDD 經過 Wide Transformations 的轉換後，原來 partition 的資料會經過 shuffle 分散到不同的 partition 去，屬於一對多的關係．相關操作有 groupByKey、reduceByKey、distinct、join．
join 操作，如果假設 RDD A 有 4 個 partition，RDD B 有 2 個 partition，A 和 B 做 join，會先把 RDD A 做一次 shuffle 把 4 個 partition 變成 2 個 partition，接著再對已經變成 2 個 partition 的 RDD A 和 RDD B 做 shuffle，總共做了 2 次 shuffle．

#### stage
spark 的 stage 有分兩種，一種是 spark job 最後產生結果的階段 ResultStage，另一種是中間過程產生的 ShuffleMapStage．
屬於 ResultStage 的 Task 都是 ResultTask，屬於 ShuffleMapStage 都是 ShuffleMapTask．

#### shuffle
shuffle 主要分為兩個階段 shuffle write 和 shuffle read．
shuffle 過程會把前一個 stage 的 shuffleMapTask 進行 shuffle write，把資料存在 blockManager 上，並且把資料位置的訊息傳給 driver 的 mapOutTrack 裡．
下一個 stage 會根據資料位置的訊息進行 shuffle read 並且拉取上個 stage 輸出的資料．
shuffle 調整參數和 Default 值 : 
```
spark.shuffle.file.buffer            32k (shuffle write)
spark.reducer.maxSizeInFlight        48m (shuffle read)
spark.reducer.maxReqsInFlight        Int.MaxValue
spark.maxRemoteBlockSizeFetchToMem   Int.MaxValue - 512 (enabled external shuffle service)
spark.shuffle.memoryFraction         0.2 (shuffle read)
```

* Int.MaxValue 大約 2 G

如果有看到要調整 spark.yarn.executor.memoryOverhead ，這參數是舊版的設定值，新版的已經改成 spark.executor.memoryOverhead．可以這樣調整．
```
--conf spark.executor.memoryOverhead=2048
```

如果有遇到
```
ERROR TaskSetManager: Total size of serialized results of 30 tasks (1108.5 MB) is bigger than spark.driver.maxResultSize (1024.0 MB)
```
可以嘗試加大 maxResultSize
```
--conf spark.driver.maxResultSize=2G
```

## Resource Allocation

影響到 spark application 的效能需要考慮到 CPU、memory、diosk IO、network IO 當然還有程式的寫法盡量避免太多 shuffle．

當要執行一個 spark application 時，這邊使用的是跑在 hadoop cluster 的 yarn 上面．需要根據 cluster 的資源來思考要怎麼分配給 spark executor 使用作運算，指令如下

```
spark-submit --class com.mitwm.poc.CustomOperate --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 15g --executor-cores 5 --num-executors 12 enrich-5.0.3.jar 
```

#### core

core 數影響的是 executor task 的並行執行的數量，所以 ` --executor-cores 5 ` 表示一個 executor 最多能有 5 個 task 同時在執行．另外 HDFS client 最好的 throughput 是一個 executor 最多 5 個 task．

#### memory

memory 影響的是能有多少量的資料可以存放在 executor 裡，存放的類型有 cache data、shuffle data．但要注意設太大時 GC 的時間可能也會增加太多．一個 executor 建議的最大值不要超過 64 G．

#### evaluate executor resource

假設有 6 台機器，每台機器有 16 cores 和 64 G 的 memory．可以用 15 個 cores 和 63 G 的資源來做 hadoop cluster．  
所以 hadoop cluster 的資源總共會有 15 * 6 = 90 cores 和 63 G * 6 = 378 G 的 memory．  
這時要設定 yarn 的兩個參數  yarn.nodemanager.resource.memory-mb 和  yarn.nodemanager.resource.cpu-vcores．

```
yarn.nodemanager.resource.memory-mb 設定 63 G * 1024 = 64512 M．
yarn.nodemanager.resource.cpu-vcores 設定 15．
```

根據上面的資源情況，spark executor 的資源這樣分配，每台跑一個 executor 每個 executor 15 cores 和 63G 的 memory．

```
--num-executors 6 --executor-cores 15 --executor-memory 63G
```

看似合理但其實會有遇到一些問題，
* 因為 executor memory 還會需要一些 overhead，所以當 63G 的 memory 都給 executor 使用就沒有多的 memory 給 overhead．
* spark driver 會被丟到其中一台機器執行，但也沒有額外多的 core 和 memory 給 driver 使用了．
* HDFS IO throughput 最好的是一個 executor 給 5 個 core．

#### executor-cores
要改善上面的值 core 數就給 5 因為 HDFS IO throughput 最好．

#### num-executors
總共有 90 個 cores / 5 (一個 executor) = 18 (總共最多可以有 18 個 executor) - 1 (for driver) = 17 executor (17 個 executor 平行度最佳)

#### executor-memory
15(一台機器 15 core) / 5(一個 executor) = 3 (一台機器最多能有 3 個 executor)

63 G / 3 = 21 G．
計算 overhead memory : 21 G * 0.1 = 2.1 G，大於 384 M．所以 overhead memory 是 2.1 G．
最後將 21 G - 2.1 G = 18.9 ~ 18 G．  
記得 driver memory 也會有 overhead memory 也是用這樣的方式算．假設 driver memroy 給 5 G，overhead 就是 5 * 0.1 = 0.5 G，實際用到的 memory 會是 5.5 G．  
0.1 這個值是參考 [spark 2.3.1 configuration](https://spark.apache.org/docs/2.3.1/configuration.html#viewing-spark-properties) 來的，建議不同版本的話要確認一下．

所以最後 spark executor 資源分配改成

```
--num-executors 17 --executor-cores 5 --executor-memory 18G
```

但實際上測試時如果 executor-memory 造上面的公式計算，job 還是有可能會 failed，感覺還是要多留一些 buffer ．所以如果出現 failed 18 G 在降個 1 ~ 2 G 試試看．

#### partitions
spark 會把要處理的資料分散在不同的 partition 裡，一個 task 可以處理一個 partition．partition 的切法可以透過 `spark.default.parallelism` 值設定．  
如果是讀取 HDFS 的檔案的話則會根據 HDFS 的 block size 來當作 partitions，所以 HDFS 的 block size 如果是 128 M，則 spark 讀取後每個 partition 也就是 128 M．(128`*`1024`*`1024 , such as 134217728 for 128 MB).
```
<property>
      <name>dfs.blocksize</name>
      <value>134217728</value>
</property>
```
透過下圖可以看到讀取 HDFS，Input size 的話幾乎都是 128 M，但如果 HDFS 的檔案小於 128 M，例如 279K 則這 279 K 就會是個自一個 partition．

![sparkPerformance_1.jpg](/static/img/spark/sparkPerformance/sparkPerformance_1.jpg){:height="400px" width="800px"}

如果要確定 rdd 的 partition 數量可以用下面方式．
```
rdd.partitions().size()
```

如果 partition 數量太少，假設有 10 台機器，但 partition 卻只有 8 個的情況下，代表不會所有的機器都會用到，會造成 cpu 的浪費．而且表示這些 small task 如果使用到一些 aggregation 的操作，例如 join、reduceByKey 之類的，而且這些操作會造成 shuffle，所以在 shuffle 時 memory 會需要比較多的，如果沒分配好會容易會造成 job failed．  
所以如果想要增加 partition 數量時可以使用下列方式．
* 使用 repartition 重新分配 partition 的數量．
* 將 HDFS 的 block size 設小一點．

如果要 repartition，但需要知道要分成多少個 partition 比較適當．

```
val rdd2 = rdd1.reduceByKey(_ + _, numPartitions = X)
```

比較好的 performance 是根據 parent RDD 的 partition 數乘上 1.5 倍．

所以如果讀取 HDFS 檔案切成 119 個 partition * 1.5 = 178.5，在做 reduceByKey 時 X 值可以設成 178 看看效能是否能增加．

在通常的情況下多一點 partition 會比太少的 partition 好．這個建議跟 MapReduce 剛好相反, 因為 spark 啟動 tasks 的 overhead 相對較少．




shuffle 參考資料 :
https://www.cnblogs.com/haozhengfei/p/5fc4a976a864f33587b094f36b72c7d3.html

shuffle read 會 failed
spark-submit --class ght.mi.twm.poc.LocationUpdate --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 25g --executor-cores 5 --num-executors 20 enrich-5.0.3.jar /data/pool/stresstest/location_1T/LSR* /data/pool/stresstest/location_1T/dailyPerson

shuffle write 時就 failed 了
spark-submit --class ght.mi.twm.poc.LocationUpdate --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 35g --executor-cores 5 --num-executors 15 enrich-5.0.3.jar /data/pool/stresstest/location_1T/LSR* /data/pool/stresstest/location_1T/dailyPerson

執行時間 1.8 h
spark-submit --class ght.mi.twm.poc.LocationUpdate --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 25g --executor-cores 5 --num-executors 15 enrich-5.0.3.jar /data/pool/stresstest/location_1T/LSR* /data/pool/stresstest/location_1T/dailyPerson



還沒測
spark-submit --conf spark.reducer.maxSizeInFlight=96m --class ght.mi.twm.poc.LocationUpdate --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 25g --executor-cores 5 --num-executors 15 enrich-5.0.3.jar /data/pool/stresstest/location_1T/LSR* /data/pool/stresstest/location_1T/dailyPerson




從下列可以看出有三台機器也就是三個 node (worker)，而每個 CoarseGrainedExecutorBackend 都包含了一個 executor，所以一台 worker 啟動了 5 個 executor，每個 executor 會有 thread pool 而每個 thread 可以執行一個 task，但在 dmpn5 上面可以看到有多一個 ApplicationMaster，也就是 driver 被丟到了 dmpn5 上面．

```
test@dmpn2:~>jps
41056 NameNode
75923 CoarseGrainedExecutorBackend
41635 JournalNode
133990 Jps
42630 NodeManager
75643 CoarseGrainedExecutorBackend
76411 CoarseGrainedExecutorBackend
75642 CoarseGrainedExecutorBackend
76410 CoarseGrainedExecutorBackend
41229 DataNode
42447 ResourceManager


test@dmpn4:~>jps
140882 CoarseGrainedExecutorBackend
191794 Jps
105344 JournalNode
141571 CoarseGrainedExecutorBackend
140881 CoarseGrainedExecutorBackend
141572 CoarseGrainedExecutorBackend
60729 NodeManager
105194 DataNode
91039 QuorumPeerMain
141132 CoarseGrainedExecutorBackend


test@dmpn5:~>jps
111141 SparkSubmit
112013 ApplicationMaster
17167 NodeManager
169395 QuorumPeerMain
113075 CoarseGrainedExecutorBackend
4691 DataNode
11123 HistoryServer
113076 CoarseGrainedExecutorBackend
112279 CoarseGrainedExecutorBackend
4855 JournalNode
5047 DFSZKFailoverController
112729 CoarseGrainedExecutorBackend
170874 Jps
112442 CoarseGrainedExecutorBackend
5214 ResourceManager
```


修改 HDFS Block size

```
<property>
  <name>dfs.blocksize</name>
  <value>134217728</value>
  <!--<value>268435456</value>-->
</property>
```

/data/pool/stresstest/location_1T_128G


hadoop distcp /data/pool/stresstest/location_1T/LSR* /data/pool/stresstest/location_1T_128G

hdfs dfs -stat %o /data/pool/stresstest/location_1T_128G/*

使用 distcp 沒改變 blocksize．

```
hadoop distcp -Ddfs.block.size=134217728 /data/pool/stresstest/location_1T/LSR* /newdata/pool/stresstest/location_1T_128G
```

使用 cp 才有辦法改變 blocksize
```
hdfs dfs -cp /data/pool/stresstest/location_1T/LSR* /newdata/pool/stresstest/location_1T_128G
```

檢查 blocksize 是否更新 : 
```
test@dmpn2:/opt/hadoop-3.1.2/etc/hadoop>hdfs dfs -stat %o /newdata/pool/stresstest/location_1T_128G/*
WARNING: HADOOP_PREFIX has been replaced by HADOOP_HOME. Using value of HADOOP_PREFIX.
2019-09-23 19:05:45,245 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
134217728
```



```
yarn node -list
```


spark.reducer.maxSizeInFlight



