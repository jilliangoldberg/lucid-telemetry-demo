package lucid.telemetry

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** A legacy telemetry job. `transform` holds all the business logic, so the same code
  * runs in streaming (StreamingMain) and in batch (GenerateExpected).
  */
trait TelemetryJob {
  def name: String
  def eventType: String
  def key: Seq[String] // one output row per key
  def transform(events: DataFrame): DataFrame
}

object Jobs {
  val all: Seq[TelemetryJob] = Seq(OtaStatusJob, BatteryHealthJob, ChargingSessionsJob)
  def byName(name: String): TelemetryJob =
    all.find(_.name == name).getOrElse(sys.error(s"unknown job: $name"))
}

/** Latest OTA status per (vin, ota_version), plus how many status updates arrived.
  * Already ported to Rust: rust/src/jobs/ota_status.rs
  */
object OtaStatusJob extends TelemetryJob {
  val name = "ota_status"
  val eventType = "ota_status"
  val key = Seq("vin", "ota_version")

  def transform(events: DataFrame): DataFrame = events
    .dropDuplicates("event_id") // Kafka can redeliver an event
    .groupBy("vin", "ota_version")
    // max of a struct compares field by field, so this picks the row with the highest seq:
    // "latest" means latest by the vehicle's sequence number, not by arrival order.
    .agg(
      max(struct(col("seq"), col("status"), col("ts_ms"))).as("latest"),
      count(lit(1)).as("updates"))
    .select(
      col("vin"),
      col("ota_version"),
      col("latest.status").as("status"),
      col("latest.seq").as("last_seq"),
      col("latest.ts_ms").as("updated_ts_ms"),
      col("updates"))
}

/** Battery health per VIN per 5-minute window, with an alert flag for service. */
object BatteryHealthJob extends TelemetryJob {
  val name = "battery_health"
  val eventType = "battery"
  val key = Seq("vin", "window_start_ms")
  val WindowMs = 5 * 60 * 1000L

  def transform(events: DataFrame): DataFrame = events
    .dropDuplicates("event_id")
    .withColumn("window_start_ms", floor(col("ts_ms") / WindowMs) * WindowMs) // tumbling window
    .groupBy("vin", "window_start_ms")
    .agg(
      count(lit(1)).as("events"),
      avg("soc_pct").as("avg_soc_pct"),
      max("temp_c").as("max_temp_c"),
      max("cell_delta_mv").as("max_cell_delta_mv"))
    .withColumn("alert", col("max_temp_c") > 45.0 || col("max_cell_delta_mv") > 50)
}

/** One row per charging session: duration, energy, charger and coarse location. */
object ChargingSessionsJob extends TelemetryJob {
  val name = "charging_sessions"
  val eventType = "charging"
  val key = Seq("vin", "session_id")

  def transform(events: DataFrame): DataFrame = {
    val isStart = col("phase") === "start"
    events
      .dropDuplicates("event_id")
      // Charger and location come from the session's "start" event (one per session).
      .withColumn("start_charger", when(isStart, col("charger_id")))
      .withColumn("start_cell", when(isStart, Privacy.locationCell(col("lat"), col("lon"))))
      .groupBy("vin", "session_id")
      .agg(
        min("start_charger").as("charger_id"),
        min("ts_ms").as("start_ts_ms"),
        max("ts_ms").as("end_ts_ms"),
        sum(coalesce(col("kwh"), lit(0.0))).as("energy_kwh"),
        min("start_cell").as("location_cell"))
  }
}
