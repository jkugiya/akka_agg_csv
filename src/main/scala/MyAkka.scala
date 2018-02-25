import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._

object Main extends App {
  implicit val system = ActorSystem("MyAkkaActor")
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withDispatcher("line-processor"))
  val path = args(0)
  val s = scala.io.Source.fromFile(path, "UTF-8")
  val source = Source.fromIterator(() => s.getLines())
  val acc_empty = Map.empty[String, Int]
  val grp_col = "lastname"
  // 行の処理。CsaParsingは高級でやや重いようなので使わない
  val processCsv = Flow[String]
      .map(_.split(","))
      .filter(_.length > 2)
      .collect {
        case Array(_, lastname, _*) =>
          lastname
      }
  // 集計処理
  val aggregate = Flow[String]
      .groupBy(30, a => a(0)) // 頭一文字でグループ分け
      .fold(acc_empty) { (acc: Map[String, Int], word: String) =>
    val cnt = acc.getOrElse(word, 0)
    acc.updated(word, cnt + 1)
  }.mergeSubstreams
   .via(Flow[Map[String, Int]].fold(acc_empty) { (acc: Map[String, Int], rec: Map[String, Int]) =>
     acc ++ rec.map { case (k, v) => k -> (v + acc.getOrElse(k, 0)) }
   })
  val graph = RunnableGraph.fromGraph(GraphDSL.create(source, Sink.last[Map[String, Int]])(Keep.right) { implicit
  builder =>
    (in, out) =>
      import GraphDSL.Implicits._
      in ~> Flow[String].buffer(10000, OverflowStrategy.backpressure) ~> processCsv ~> aggregate ~> out
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
    system.terminate()
  }
}
