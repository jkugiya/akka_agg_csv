import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl._
import akka.util.ByteString


object Main extends App {
  implicit val system = ActorSystem("MyAkkaActor")
  implicit val materializer = ActorMaterializer()
  implicit val dipatcher = system.dispatcher


  // val path = if (params.isEmpty) "../fukuokaex/test_12_000_000.csv" else params(0)
  //val path = "../fukuokaex/test_3_000_000.csv"
  val path = args(0)
  val source = FileIO.fromPath(Paths.get(path))

  val acc_empty = Map.empty[String, Int]
  var resultMap = Map.empty[String, Int]
  val grp_col = "lastname"

  val start = System.nanoTime()
  source
    .via(CsvParsing.lineScanner())
    .via(CsvToMap.withHeaders("firstname", "lastname", "gender", "birthday", "addr1", "addr2", "addr3", "state", "email", "zip", "tel", "attr", "regdate"))
    .map(_.map(r => (r._1, r._2.utf8String))) // Map( birthday ->'1950/01/20', email -> 'enpedasi@heaven.com' )
    .filter(rec => rec.get(grp_col) != None)
    .groupBy(30, r => r(grp_col)(0)) // 頭一文字でグループ分け
    .async
    .fold(acc_empty) { (acc: Map[String, Int], rec: Map[String, String]) =>
      val word = rec(grp_col)
      val cnt = acc.getOrElse(word, 0)
      acc.updated(word, cnt + 1)
    }
    .mergeSubstreams
    .via(Flow[Map[String, Int]].fold(acc_empty) { (acc: Map[String, Int], rec: Map[String, Int]) =>
      acc ++ rec.map { case (k, v) => k -> (v + acc.getOrElse(k, 0)) }
    })
    .runWith(Sink.foreach(e => {
      resultMap = e
    }))
    .onComplete(done ⇒ {
      val nsec = (System.nanoTime() - start)
      println(done)
      resultMap.toSeq.sortWith(_._2 > _._2).take(10).foreach(println)
      println(nsec + "nsec")
      println((nsec / 1000000) + "msec")
      system.terminate()
    })
}
