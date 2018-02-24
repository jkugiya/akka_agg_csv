import java.nio.file.Files

import scala.io.Source

object NoAkka {
  def main(args: Array[String]): Unit = {
    val source = Source.fromFile(args(0), "UTF-8")
    val start = System.nanoTime()
    val result = source.getLines()
        .grouped(1000) // Seq[String]
        .grouped(8) // Seq[Seq[String]]
        .flatMap { linesSeq =>
      linesSeq.par.map { lines =>
        for {
          line <- lines
          splited = line.split(",")
          lastname = splited(1)
          if lastname.nonEmpty
        } yield lastname
      }
    }.flatMap(identity)
        .grouped(30)
        .map(_.groupBy(a => a(0)))
        .flatMap { grouped =>
          grouped.values.par.map { words =>
            words.foldLeft(Map.empty[String, Int]) {
              case (acc, word) =>
                val cnt = acc.getOrElse(word, 0)
                acc.updated(word, cnt + 1)
            }
          }
        }.foldLeft(Map.empty[String, Int]) {
      case (acc, rec) =>
        acc ++ rec.map { case (k, v) => k -> (v + acc.getOrElse(k, 0)) }
    }
    val nsec = (System.nanoTime() - start)
    result.toSeq.sortWith(_._2 > _._2).take(10).foreach(println)
    println(nsec + "nsec")
    println((nsec / 1000000) + "msec")
  }
}
