package lucid.telemetry

import org.apache.spark.sql.types._

/** Schema of one vehicle-to-cloud telemetry event (one JSON message on the Kafka topic).
  * Every event has the common fields; the rest depend on `type`.
  */
object Events {
  val schema: StructType = StructType(Seq(
    // Common to every event
    StructField("event_id", StringType),   // unique per event; Kafka may redeliver it
    StructField("vin", StringType),
    StructField("seq", LongType),          // per-VIN sequence number set by the vehicle
    StructField("ts_ms", LongType),        // event time, epoch millis
    StructField("type", StringType),       // "ota_status" | "battery" | "charging"
    // OTA update status
    StructField("ota_version", StringType),
    StructField("status", StringType),
    // Battery health
    StructField("soc_pct", DoubleType),
    StructField("temp_c", DoubleType),
    StructField("cell_delta_mv", LongType),
    // Charging sessions
    StructField("session_id", StringType),
    StructField("phase", StringType),      // "start" | "progress" | "end"
    StructField("charger_id", StringType),
    StructField("kwh", DoubleType),
    // GPS location, present on every event. VIN-linked, so never output raw (see Privacy).
    StructField("lat", DoubleType),
    StructField("lon", DoubleType)
  ))
}
