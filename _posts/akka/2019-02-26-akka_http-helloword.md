---
layout: post
comments: false
categories: akka
---

### akka-http

可以參考官網的 [akka-http](https://doc.akka.io/docs/akka-http/current/introduction.html#philosophy)

```
package ght.mi.enrich.rest

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer

import scala.io.StdIn

object SimpleWebServer {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val route =
      path("hello") {
        get {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
        }
      }

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}

```
接著只要發一個 http request 就可以收到 response 的內容．  

```
> curl http://localhost:8080/hello
<h1>Say hello to akka-http</h1>
```
但由於上面的程式只有實作 get 方法．所以使用 post 方式會收到 Unsupported HTTP method．  

```
curl -X post http://localhost:8080/hello
Unsupported HTTP method
```
如果 request 的 url 不存在時就會收到下列訊息．  

```
curl http://localhost:8080/
The requested resource could not be found.
```
















