import java.nio.file.Files

import org.apache.commons.lang3.StringUtils

import scala.io.Source

object NoAkka {
  def main(args: Array[String]): Unit = {
    val source = Source.fromFile(args(0), "UTF-8")
    val start = System.nanoTime()
    val result = source.getLines()
        .map(StringUtils.split(_, ",", 3)(1))
        .filter(_.nonEmpty)
        .grouped(100000)
        .toStream
        .par
        .map(_.groupBy(_.hashCode()))
        .map { grouped =>
          grouped.values.map { words =>
            words.foldLeft(Map.empty[String, Int]) {
              case (acc, word) =>
                val cnt = acc.getOrElse(word, 0)
                acc.updated(word, cnt + 1)
            }
          }
        }.map(_.foldLeft(Map.empty[String, Int])(_ ++ _))
        .foldLeft(Map.empty[String, Int]) {
      case (acc, rec) =>
        acc ++ rec.map { case (k, v) => k -> (v + acc.getOrElse(k, 0)) }
    }
    val nsec = (System.nanoTime() - start)
    result.toSeq.sortWith(_._2 > _._2).take(10).foreach(println)
    println(nsec + "nsec")
    println((nsec / 1000000) + "msec")
  }
}
