import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.AnyRefMap

object Main extends App {
  implicit val system = ActorSystem("MyAkkaActor")
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withDispatcher("line-processor"))
  val path = args(0)
  val s = scala.io.Source.fromFile(path, "UTF-8")
  val source = Source.fromIterator(() => s.getLines())
  val grp_col = "lastname"
  // 行の処理。CsaParsingは高級でやや重いようなので使わない
  val processCsv = Flow[String]
      .map(s => StringUtils.split(s, ",", 3)(1))// 速い実装を使う
  // 集計処理
  val aggregate = Flow[String]
      .groupBy(1000, s => Math.abs(s.hashCode)  % 30)
      .fold(AnyRefMap.empty[String, Int]) { (acc, word) =>
    acc.updated(word, acc.getOrElse(word, 0) + 1)
  }.mergeSubstreams
   .via(Flow[AnyRefMap[String, Int]].fold(Map.empty[String, Int]) { (acc, rec) =>
     acc ++ rec
   })
  val graph = RunnableGraph.fromGraph(GraphDSL.create(source, Sink.last[Map[String, Int]])(Keep.right) { implicit
  builder =>
    (in, out) =>
      import GraphDSL.Implicits._
      in ~>  processCsv ~> aggregate ~> out
      ClosedShape
  })

  import system.dispatcher

  val start = System.nanoTime()
  graph.run().onComplete { done =>
    val nsec = (System.nanoTime() - start)
    done.foreach { resultMap =>
      resultMap.toSeq.sortWith(_._2 > _._2).take(10).foreach(println)
    }
    done.failed.foreach(println)
    println(nsec + "nsec")
    println((nsec / 1000000) + "msec")
    s.close()
    system.terminate()
  }
}
