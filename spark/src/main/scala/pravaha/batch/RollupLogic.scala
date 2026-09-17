package pravaha.batch

import java.sql.Timestamp
import java.time.Instant
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** The actual aggregation Pravaha's batch layer computes, and why.
  *
  * Pravaha's real-time speed layer (`pravaha.analytics.processor`) already
  * emits per-minute metrics from a Redis-backed, watermark-driven windower -
  * fast, but an *approximation*: it classifies lateness with a heuristic
  * watermark and only holds state for a bounded grace period before flushing.
  *
  * This batch job is the other half of a Lambda architecture: it re-derives
  * the same kind of numbers directly from the immutable, fully-durable
  * `events` table (Pravaha's actual event store - see
  * docs/decisions/0009-event-storage.md) with no watermark heuristics and no
  * time pressure, at hourly grain, so it can be trusted as the reconciled,
  * corrected-after-the-fact view: exactly what a batch layer is *for*.
  *
  * Per (event_type, region, hour_bucket) it computes:
  *   - event_count               total events
  *   - distinct_correlation_ids  distinct business transactions touched
  *   - distinct_users            distinct payload.user_id seen (nulls ignored)
  *   - total_amount / avg_amount revenue rollup, from the same `amount`
  *                               column the real-time layer uses
  *   - late_event_count          events NOT classified "on_time" at ingest -
  *                               a genuine, exact recount of the real-time
  *                               watermark's late/too-late calls, useful for
  *                               auditing how aggressive the watermark is
  *   - replay_event_count        events that arrived via the replay engine
  */
object RollupLogic {

  /** Input schema this expects (produced by [[EventRollupJob]]'s JDBC read):
    *   event_type: String, region: String, hour_bucket: Timestamp,
    *   correlation_id: String, user_id: String (nullable),
    *   amount: Double (nullable), is_late: Int (0/1), is_replay: Int (0/1)
    */
  val InputSchema: StructType = StructType(
    Seq(
      StructField("event_type", StringType, nullable = false),
      StructField("region", StringType, nullable = false),
      StructField("hour_bucket", TimestampType, nullable = false),
      StructField("correlation_id", StringType, nullable = false),
      StructField("user_id", StringType, nullable = true),
      StructField("amount", DoubleType, nullable = true),
      StructField("is_late", IntegerType, nullable = false),
      StructField("is_replay", IntegerType, nullable = false),
    )
  )

  /** Pure(ish) aggregation: raw event rows -> one row per (event_type,
    * region, hour_bucket). `computedAt`/`windowFrom`/`windowTo` are stamped
    * onto every output row for auditability of which batch run produced it.
    */
  def aggregate(
      events: DataFrame,
      computedAt: Instant,
      windowFrom: Instant,
      windowTo: Instant,
  ): DataFrame = {
    val computedAtTs = Timestamp.from(computedAt)
    val windowFromTs = Timestamp.from(windowFrom)
    val windowToTs = Timestamp.from(windowTo)

    events
      .groupBy(col("event_type"), col("region"), col("hour_bucket"))
      .agg(
        count(lit(1)).as("event_count"),
        countDistinct(col("correlation_id")).as("distinct_correlation_ids"),
        countDistinct(col("user_id")).as("distinct_users"),
        coalesce(sum(col("amount")), lit(0.0)).as("total_amount"),
        coalesce(avg(col("amount")), lit(0.0)).as("avg_amount"),
        coalesce(sum(col("is_late")), lit(0L)).as("late_event_count"),
        coalesce(sum(col("is_replay")), lit(0L)).as("replay_event_count"),
      )
      .withColumn("id", expr("uuid()"))
      .withColumn("window_from", lit(windowFromTs))
      .withColumn("window_to", lit(windowToTs))
      .withColumn("computed_at", lit(computedAtTs))
      .select(
        col("id"),
        col("event_type"),
        col("region"),
        col("hour_bucket"),
        col("event_count").cast(LongType),
        col("distinct_correlation_ids").cast(LongType),
        col("distinct_users").cast(LongType),
        col("total_amount").cast(DoubleType),
        col("avg_amount").cast(DoubleType),
        col("late_event_count").cast(LongType),
        col("replay_event_count").cast(LongType),
        col("window_from"),
        col("window_to"),
        col("computed_at"),
      )
  }
}
