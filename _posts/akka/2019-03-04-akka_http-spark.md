---
layout: post
comments: false
categories: akka
---

### akka-http call spark

目的是利用 akka http 建立一個 web service 給 client 端呼叫，收到 request 後執行啟動 spark job．  
由於 spark job 是 batch job，所以執行結果可能無法即時傳回給前端，所以可以利用一個 key 存在 cassandra，讓前端查詢該 job 執行的狀況．

![akkahttp_1.jpg](/static/img/akka/akkahttp_1.jpg){:height="400px" width="600px"}

建立 cassandra table  

```
CREATE TABLE miks2.jobList(
jobId text,
sparkJobId text,
status text,
PRIMARY KEY(jobId)
) ;
```


```
package ght.mi.enrich.rest

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.sql.SparkSession
import spray.json._

import scala.io.StdIn

final case class IdList(ids: List[String])
final case class LooklikeInfo(likePerson :String , kcluster :String , ids: List[String])

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val looklikeInfoFormat = jsonFormat3(LooklikeInfo)
}

// 要 extends JsonSupport
object SimpleWebServer extends JsonSupport {

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    /*
    * http://192.168.6.31:8080/lookLikePersonWithFilePath?likePerson=100&hdfsCsvFile=/enrich/looklike/idlist/idlist_5.csv&kcluster=5
    * */
    val helloRoute = path("lookLikePersonWithFilePath") {
      get {
        // 設定 request 接收的參數
        parameters('likePerson.as[String],'hdfsCsvFile.as[String],'kcluster.as[String] ) {
          (likePerson , hdfsCsvFile , kcluster) =>
            val id = java.util.UUID.randomUUID().toString
            /*
            * spark-submit --class ght.mi.ml.KmeansLookLikePerson --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 20g --executor-cores 6 --num-executors 5 enrich-5.0.jar 1000
            * */
            val sparkLauncher = new SparkLauncher()
              .setAppResource("/home/miuser/enrich/genData/enrich-5.0.jar")
              .setMainClass("ght.mi.ml.KmeansLookLikePerson")
              .setMaster("yarn")
              .setDeployMode("cluster")
              .setConf("spark.driver.memory", "4g")
              .setConf("spark.executor.memory", "20g")
              .setConf("spark.executor.instances", "5")
              .setConf("spark.executor.cores", "6")
              .setConf("spark.driver.allowMultipleContexts", "true")
              .addAppArgs(likePerson , hdfsCsvFile , kcluster , id)
              .launch()

            //會等 job 執行完才會 complete
            //sparkLauncher.waitFor()

            complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, id))
        }
      }
    }

    val jsonRoute = post {
      path("lookLikePerson") {
        // 使用 json 格式的方式接收 request，再取出 json 的值
        entity(as[LooklikeInfo]) { looklikeInfo =>
          val id = java.util.UUID.randomUUID().toString
          val ids = looklikeInfo.ids.mkString(",")
          val likePerson = looklikeInfo.likePerson
          val kcluster = looklikeInfo.kcluster
          /*
           * spark-submit --class ght.mi.ml.KmeansLookLikePerson --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 20g --executor-cores 6 --num-executors 5 enrich-5.0.jar 1000
           * */
          val sparkLauncher = new SparkLauncher()
            .setAppResource("/home/miuser/enrich/genData/enrich-5.0.jar")
            .setMainClass("ght.mi.ml.KmeansLookLikePersonWithJson")
            .setMaster("yarn")
            .setDeployMode("cluster")
            .setConf("spark.driver.memory", "4g")
            .setConf("spark.executor.memory", "20g")
            .setConf("spark.executor.instances", "5")
            .setConf("spark.executor.cores", "6")
            .setConf("spark.driver.allowMultipleContexts", "true")
            .addAppArgs(likePerson, ids, kcluster, id)
            .launch()

          complete(HttpEntity(ContentTypes.`application/json`, "{\"jobid\" : " + id + " }"))
        }
      }
    }

    /*
    * http://192.168.6.31:8080/getJobStatus?jobId=42252e51-ded4-461e-8800-db3bcc04ed35
    * */
    val jobStatusRoute = path("getJobStatus") {
      get {
        parameters('jobId.as[String]) {
          (jobId) =>

            val spark = SparkSession.builder()
              .appName("LookLikePerson")
              .master("local[*]")
              .config("spark.cassandra.connection.host", "192.168.6.31,192.168.6.32,192.168.6.33")
              .config("spark.cassandra.auth.username", "miuser")
              .config("spark.cassandra.auth.password", "mimimi123")
              .getOrCreate()

            import com.datastax.spark.connector._
            val jobStatus = spark.sparkContext.cassandraTable[(String,String,String)]("miks2","joblist")
              .select("jobid","sparkjobid","status").where("jobid = ? " , jobId).keyBy[Tuple1[String]]("jobid")

            complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, jobStatus.first()._2._3))
        }
      }
    }

    val route = {
      helloRoute ~ jsonRoute ~ jobStatusRoute
    }

    val bindingFuture = Http().bindAndHandle(route, "192.168.6.31", 8080)

    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}

```


build.sbt :  

```
name := "enrich-rest"

version := "0.1"

scalaVersion := "2.11.12"

val akkaVersion = "2.5.19"
val akkaHttpVersion = "10.1.7"
val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark"       %%  "spark-sql"                   % sparkVersion ,
  "org.apache.spark"       %%  "spark-core"                  % sparkVersion ,
  "com.typesafe.akka"      %%  "akka-http"                   % akkaHttpVersion,
  "com.typesafe.akka"      %%  "akka-stream"                 % akkaVersion,
  "com.typesafe.akka"      %%  "akka-http-spray-json"        % "10.1.7",
  "com.datastax.spark"     %%  "spark-cassandra-connector"   % "2.3.2",
  "com.typesafe.akka"      %%  "akka-http-testkit"           % akkaHttpVersion % Test,
  "com.typesafe.akka"      %%  "akka-stream-testkit"         % akkaVersion     % Test
)

parallelExecution in Test := false

assemblyJarName in assembly := name.value + ".jar"

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("reference.conf") => MergeStrategy.concat
  case x => MergeStrategy.first
}

```


