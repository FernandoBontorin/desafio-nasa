package nasa

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NasaJob {
  val spark = SparkSession.builder().getOrCreate()
  val splitHttpLine = udf((line: String) => {
    def bytesToLong(s: String) = if (s == "-") "0" else s
    val pattern =
      "(.+) - - \\[(.+)\\] \"(\\w+) (.+) (\\w+/\\d+[.]\\d+)\" ([0-9]+) (-|[0-9]+)".r
    val pattern(host, date, method, path, protocol, code, bytes) = line
    Array(host, date, method, path, protocol, code, bytesToLong(bytes))
  })
  val isoFormat = udf((date: String) => {
    import java.text.SimpleDateFormat
    def dateFix = date.replace("Aug", "Ago")
    val dateParser = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")
    val iso = new SimpleDateFormat("yyyy-MM-dd")
    iso.format(dateParser.parse(dateFix))
  })

  def main(args: Array[String]): Unit = {
    val data = args.map(spark.read.textFile).reduce(_ union _)
    val reqSplitted = data.select(splitHttpLine(col("value")).as("req"))
    val reqStruct = reqSplitted.select(
      col("req")(0).as("host"),
      col("req")(1).as("date"),
      col("req")(2).as("path"),
      col("req")(3).as("code"),
      col("req")(4).as("bytes")
    ).cache
    val uniqueHostsCount = reqStruct
      .groupBy(col("host"))
      .count
      .collect
      .map(row => row.getAs[String]("host"))
      .length
    val codes404Count = reqStruct.where(col("code") === 404).count
    val top5Hosts404 = reqStruct
      .where(col("code") === 404)
      .groupBy("host")
      .count
      .sort(col("count").desc)
      .collect
      .take(5)
      .map(row => row.getAs[String]("host"))
    val amount404PerDay = reqStruct
      .where(col("code") === 404)
      .select(isoFormat(col("date")).as("dateIso"))
      .groupBy("dateIso")
      .count
      .collect
      .map(row => (row.getAs[String]("dateIso"), row.getAs[Long]("count")))
    val totalBytesReturned = reqStruct
      .select(sum("bytes").as("bytes"))
      .collect
      .head
      .getAs[Double]("bytes")
      .toLong

    println(s"Número de hosts únicos. $uniqueHostsCount")
    println(s"O total de erros 404. $codes404Count")
    println(
      s"Os 5 URLs que mais causaram erro 404. ${top5Hosts404.mkString(",")}"
    )
    println(
      s"Quantidade de erros 404 por dia. ${amount404PerDay.mkString(",")}"
    )
    println(s"O total de bytes retornados. $totalBytesReturned")
  }

}
