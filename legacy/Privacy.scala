package lucid.telemetry

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.format_string

/** The approved location pattern (privacy/POLICY.md): VIN-linked location may leave a job
  * only as a ~1 km cell, i.e. lat/lon to 2 decimals. Raw coordinates never leave a job.
  */
object Privacy {
  def locationCell(lat: Column, lon: Column): Column = format_string("%.2f,%.2f", lat, lon)
}
