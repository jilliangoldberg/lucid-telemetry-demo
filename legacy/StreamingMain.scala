package lucid.telemetry

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Production entrypoint (not run in this demo): reads the telemetry topic from Kafka,
  * applies one job's transform and writes results to that job's output topic.
  * Usage: StreamingMain <job-name> <kafka-brokers>
  */
object StreamingMain {
  def main(args: Array[String]): Unit = {
    val Array(jobName, brokers) = args
    val job = Jobs.byName(jobName)
    val spark = SparkSession.builder.appName(s"telemetry-$jobName").getOrCreate()

    val events = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", "vehicle-telemetry")
      .load()
      .select(from_json(col("value").cast("string"), Events.schema).as("e"))
      .select("e.*")
      .where(col("type") === job.eventType)

    job.transform(events)
      .select(to_json(struct(col("*"))).as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", s"telemetry.$jobName")
      .option("checkpointLocation", s"/checkpoints/$jobName") // offsets + state survive restarts
      .outputMode("update")
      .start()
      .awaitTermination()
  }
}
