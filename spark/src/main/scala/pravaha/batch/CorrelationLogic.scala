package pravaha.batch

import java.sql.Timestamp
import java.time.Instant
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Cross-event-type correlation summary - the second, deliberately different
  * half of Pravaha's batch layer (alongside [[RollupLogic]]'s hourly rollup).
  *
  * Why this is a genuinely different computation, not another re-slice of the
  * same numbers: Pravaha's real-time speed layer
  * (`pravaha.analytics.processor`) maintains a *bounded* 1-minute tumbling
  * window per metric and flushes it once the watermark passes - it can tell
  * you "404 payment.failed events happened in this minute" but it cannot,
  * without unbounded state, also tell you "across every minute we've ever
  * seen, how strongly does payment.failed volume move together with
  * order.cancelled volume". That needs random access across the *entire*
  * history at once, which is exactly what a single batch pass over the
  * durable `events` table can do cheaply and a stateful streaming operator
  * structurally cannot.
  *
  * What it computes: for every unordered pair of event types, the Pearson
  * correlation coefficient and sample covariance of their per-bucket event
  * counts, over every `bucketMinutes`-sized bucket in the requested window -
  * including buckets where one side of the pair had zero events (a dense
  * grid, not just the buckets where both happened to co-occur, which would
  * silently bias the statistic).
  */
object CorrelationLogic {

  /** Input schema this expects (produced by [[EventCorrelationJob]]'s JDBC
    * read): event_type: String, bucket: Timestamp (already truncated to the
    * requested bucket size).
    */
  val InputSchema: StructType = StructType(
    Seq(
      StructField("event_type", StringType, nullable = false),
      StructField("bucket", TimestampType, nullable = false),
    )
  )

  /** events -> one row per unordered (event_type_a, event_type_b) pair.
    * event_type_a < event_type_b lexicographically, so each pair appears once
    * and no type is correlated against itself.
    */
  def aggregate(
      events: DataFrame,
      bucketMinutes: Int,
      computedAt: Instant,
      windowFrom: Instant,
      windowTo: Instant,
  ): DataFrame = {
    val spark = events.sparkSession
    import spark.implicits._

    val perBucketType = events
      .groupBy(col("bucket"), col("event_type"))
      .agg(count(lit(1)).as("cnt"))

    val allBuckets = perBucketType.select("bucket").distinct()
    val allTypes = perBucketType.select("event_type").distinct()

    // Dense (bucket x event_type) grid: every event type gets an explicit 0
    // for buckets it didn't occur in, so the correlation is computed over the
    // *same* n for every pair, not just the buckets where both happened to
    // have at least one event.
    val denseGrid = allBuckets
      .crossJoin(allTypes)
      .join(perBucketType, Seq("bucket", "event_type"), "left")
      .withColumn("cnt", coalesce(col("cnt"), lit(0L)))

    val left = denseGrid.as("a")
    val right = denseGrid.as("b")

    val pairs = left
      .join(right, $"a.bucket" === $"b.bucket" && $"a.event_type" < $"b.event_type")
      .select(
        $"a.event_type".as("event_type_a"),
        $"b.event_type".as("event_type_b"),
        $"a.bucket".as("bucket"),
        $"a.cnt".as("cnt_a"),
        $"b.cnt".as("cnt_b"),
      )

    val computedAtTs = Timestamp.from(computedAt)
    val windowFromTs = Timestamp.from(windowFrom)
    val windowToTs = Timestamp.from(windowTo)

    pairs
      .groupBy(col("event_type_a"), col("event_type_b"))
      .agg(
        count(lit(1)).as("n_buckets"),
        avg(col("cnt_a")).as("mean_count_a"),
        avg(col("cnt_b")).as("mean_count_b"),
        corr(col("cnt_a"), col("cnt_b")).as("pearson_r"),
        covar_samp(col("cnt_a"), col("cnt_b")).as("covariance"),
      )
      .withColumn("id", expr("uuid()"))
      .withColumn("bucket_minutes", lit(bucketMinutes))
      .withColumn("window_from", lit(windowFromTs))
      .withColumn("window_to", lit(windowToTs))
      .withColumn("computed_at", lit(computedAtTs))
      .select(
        col("id"),
        col("event_type_a"),
        col("event_type_b"),
        col("bucket_minutes"),
        col("n_buckets").cast(LongType),
        col("mean_count_a").cast(DoubleType),
        col("mean_count_b").cast(DoubleType),
        col("pearson_r").cast(DoubleType),  // NULL when either series has zero variance - see below
        col("covariance").cast(DoubleType),
        col("window_from"),
        col("window_to"),
        col("computed_at"),
      )
  }
}
