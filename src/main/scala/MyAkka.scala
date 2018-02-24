import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.{ ActorAttributes, ActorMaterializer, ClosedShape, OverflowStrategy }
import akka.stream.alpakka.csv.scaladsl.{ CsvParsing, CsvToMap }
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.Future


object Main extends App {
  implicit val system = ActorSystem("MyAkkaActor")
  implicit val materializer = ActorMaterializer()
  // implicit val dipatcher = system.dispatcher
  // val path = if (params.isEmpty) "../fukuokaex/test_12_000_000.csv" else params(0)
  //val path = "../fukuokaex/test_3_000_000.csv"
  val path = args(0)
  val source = FileIO.fromPath(Paths.get(path))

  val acc_empty = Map.empty[String, Int]
  val grp_col = "lastname"

  val start = System.nanoTime()
  val bufferFlow = Flow[ByteString]
      .buffer(10000, OverflowStrategy.backpressure)
  val processLine = Flow[ByteString]
      .via(CsvParsing.lineScanner())
      .grouped(1000)
      .mapAsync(8) { lines =>
        val sink = Sink.fold[List[String], String](List.empty[String])((u, t) => t :: u)
        RunnableGraph.fromGraph(GraphDSL.create(Source.fromFuture(Future.successful(lines.toList)), sink)(Keep.right) { implicit b => (in, out) =>
            import GraphDSL.Implicits._
            in ~> Flow[List[List[ByteString]]].mapConcat(identity) ~>
                CsvToMap.withHeaders("firstname", "lastname", "gender", "birthday", "addr1", "addr2", "addr3", "state", "email", "zip", "tel", "attr", "regdate")
                .collect {
                  case rec if rec.get(grp_col).nonEmpty =>
                    rec(grp_col).utf8String
                } ~> out
            ClosedShape
        }).withAttributes(ActorAttributes.dispatcher("word-count")).run()
      }
  val aggregate = Flow[String]
      .groupBy(30, a => a(0)) // 頭一文字でグループ分け
      .fold(acc_empty) { (acc: Map[String, Int], word: String) =>
        val cnt = acc.getOrElse(word, 0)
        acc.updated(word, cnt + 1)
        }.mergeSubstreams
      .via(Flow[Map[String, Int]].fold(acc_empty) { (acc: Map[String, Int], rec: Map[String, Int]) =>
        acc ++ rec.map { case (k, v) => k -> (v + acc.getOrElse(k, 0)) }
      })
  val graph = RunnableGraph.fromGraph(GraphDSL.create(source, Sink.last[Map[String, Int]])(Keep.right) { implicit builder => (in, out) =>
    import GraphDSL.Implicits._
    in ~> bufferFlow ~> processLine.mapConcat(identity) ~> aggregate ~> out
    ClosedShape
  })
  import system.dispatcher
  graph.run().onComplete { done =>
    val nsec = (System.nanoTime() - start)
    done.foreach { resultMap =>
      resultMap.toSeq.sortWith(_._2 > _._2).take(10).foreach(println)
    }
    println(nsec + "nsec")
    println((nsec / 1000000) + "msec")
    system.terminate()
  }
}
