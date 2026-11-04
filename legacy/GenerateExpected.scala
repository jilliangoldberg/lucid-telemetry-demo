package lucid.telemetry

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/** Writes the parity baseline: runs each job's transform over fixtures/<job>/input.jsonl
  * and saves the result as fixtures/<job>/expected.jsonl (the evals compare Rust to this).
  *
  * Runs in batch mode. Over a complete input, batch gives the same rows as the streaming
  * job's final state, since no fixture event is later than the streaming watermark.
  * Usage (from the repo root): scala-cli run legacy --main-class lucid.telemetry.GenerateExpected
  */
object GenerateExpected {
  def main(args: Array[String]): Unit = {
    val root = Paths.get(args.headOption.getOrElse("."))
    val spark = SparkSession.builder
      .master("local[2]")
      .appName("generate-expected")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for (job <- Jobs.all) {
      val dir = root.resolve(s"fixtures/${job.name}")
      val events = spark.read.schema(Events.schema).json(dir.resolve("input.jsonl").toString)
      val rows = job.transform(events).orderBy(job.key.map(col): _*).toJSON.collect()
      Files.write(dir.resolve("expected.jsonl"), rows.mkString("", "\n", "\n").getBytes(UTF_8))
      println(s"${job.name}: ${rows.length} expected rows")
    }
    spark.stop()
  }
}
