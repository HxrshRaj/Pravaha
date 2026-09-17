package pravaha.batch

import java.sql.Timestamp
import java.time.Instant
import org.apache.spark.sql.{SaveMode, SparkSession}

/** Pravaha batch layer, second job: cross-event-type correlation summary.
  *
  * Reads the full (or a requested window of the) `events` table and computes,
  * for every pair of event types, how strongly their per-bucket volumes move
  * together over time - see [[CorrelationLogic]] for what and why. Writes to
  * `batch_event_correlations` (schema owned by the `BatchEventCorrelation`
  * SQLAlchemy model + its Alembic migration, same pattern as
  * `batch_event_rollups`).
  *
  * Unlike the hourly rollup, this is a whole-of-window summary statistic, not
  * an append-only per-hour fact: each run recomputes the correlation over its
  * entire window and replaces every row for that `bucket_minutes` setting
  * (idempotent the same way - delete then insert - just keyed on the
  * granularity rather than a time slice, because "the correlation over the
  * last N hours" is one number that gets superseded as new data arrives, not
  * a growing time series of per-hour facts).
  *
  * Usage (from spark/):
  *   sbt "runMain pravaha.batch.EventCorrelationJob"
  *   sbt "runMain pravaha.batch.EventCorrelationJob --bucket-minutes 5"
  *   sbt "runMain pravaha.batch.EventCorrelationJob --from 2026-09-08T00:00:00Z --to 2026-09-18T00:00:00Z"
  */
object EventCorrelationJob {

  private val TargetTable = "batch_event_correlations"

  def main(args: Array[String]): Unit = {
    val opts: Map[String, String] =
      args.sliding(2, 2).collect { case Array(k, v) if k.startsWith("--") => k.drop(2) -> v }.toMap

    val to = opts.get("to").map(Instant.parse).getOrElse(Instant.now())
    // Default to "all available history" - a correlation is only meaningful
    // with as many buckets as exist, unlike the rollup job's 24h default.
    val from = opts.get("from").map(Instant.parse).getOrElse(Instant.EPOCH)
    val bucketMinutes = opts.get("bucket-minutes").map(_.toInt).getOrElse(1)
    require(from.isBefore(to), s"--from ($from) must be before --to ($to)")
    require(bucketMinutes >= 1, s"--bucket-minutes must be >= 1, got $bucketMinutes")

    val jdbc = JdbcConfig.fromEnv()
    println(s"[pravaha-batch-corr] window: $from -> $to, bucket=${bucketMinutes}m")
    println(s"[pravaha-batch-corr] source: ${jdbc.url} (table: events)")

    val spark = SparkSession
      .builder()
      .appName("pravaha-event-correlation")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.ui.showConsoleProgress", "false")
      // See EventRollupJob for why this matters: Spark's JDBC path converts
      // timestamps through the session timezone unless pinned to UTC.
      .config("spark.sql.session.timeZone", "UTC")
      // Spark 4's default ANSI mode makes corr()/covar_samp() throw
      // DIVIDE_BY_ZERO for a zero-variance event type instead of returning
      // NULL (found by the test suite, not assumed) - that correlation is
      // genuinely undefined, not an error, so this restores the pre-ANSI
      // "return NULL" behaviour rather than crashing the whole job over one
      // constant-count pair.
      .config("spark.sql.ansi.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      val bucketed = readBucketedEvents(spark, jdbc, from, to, bucketMinutes)
      val rawCount = bucketed.count()
      println(s"[pravaha-batch-corr] read $rawCount bucketed event row(s) from the window")

      val pairs = CorrelationLogic.aggregate(bucketed, bucketMinutes, Instant.now(), from, to)
      val pairCount = pairs.count()
      println(s"[pravaha-batch-corr] computed $pairCount event-type pair(s)")

      pairs
        .orderBy(pairs("pearson_r").desc_nulls_last)
        .show(50, truncate = false)

      deleteExisting(jdbc, bucketMinutes)
      pairs.write.mode(SaveMode.Append).jdbc(jdbc.url, TargetTable, jdbc.properties)
      println(s"[pravaha-batch-corr] wrote $pairCount row(s) to $TargetTable")
    } finally {
      spark.stop()
    }
  }

  /** event_type + a bucket timestamp truncated to `bucketMinutes`, computed in
    * Postgres from the raw Unix epoch of `event_time` - epoch seconds are
    * inherently UTC regardless of any session/DB timezone setting, which
    * sidesteps the date_trunc/session-timezone pitfall entirely rather than
    * relying on it being configured correctly downstream.
    *
    * Bucketing is done in pure bigint arithmetic (cast to bigint, integer-
    * divide, multiply back), not `floor(epoch::double precision / n)`: found,
    * by cross-checking this job's output against an independent pandas
    * recomputation, that `extract(epoch ...)` returns a double precision, and
    * a double can't exactly represent every microsecond-precision timestamp's
    * epoch value at 2026-scale magnitudes - so a handful of events sitting
    * right on a bucket boundary could round into the adjacent bucket instead
    * of a false floating-point sliver away from it. Integer division has no
    * such rounding, so it can't misplace an event.
    */
  private def readBucketedEvents(
      spark: SparkSession,
      jdbc: JdbcConfig,
      from: Instant,
      to: Instant,
      bucketMinutes: Int,
  ) = {
    val bucketSeconds = bucketMinutes * 60
    val subquery =
      s"""(
         |  SELECT
         |    event_type,
         |    to_timestamp(
         |      (extract(epoch FROM event_time)::bigint / $bucketSeconds) * $bucketSeconds
         |    ) AS bucket
         |  FROM events
         |  WHERE event_time >= TIMESTAMPTZ '${from.toString}'
         |    AND event_time <  TIMESTAMPTZ '${to.toString}'
         |) AS bucketed_events""".stripMargin

    spark.read.jdbc(jdbc.url, subquery, jdbc.properties)
  }

  private def deleteExisting(jdbc: JdbcConfig, bucketMinutes: Int): Unit = {
    Class.forName("org.postgresql.Driver")
    val conn = java.sql.DriverManager.getConnection(jdbc.url, jdbc.user, jdbc.password)
    try {
      val stmt = conn.prepareStatement(s"DELETE FROM $TargetTable WHERE bucket_minutes = ?")
      try {
        stmt.setInt(1, bucketMinutes)
        val deleted = stmt.executeUpdate()
        println(s"[pravaha-batch-corr] cleared $deleted existing row(s) for bucket_minutes=$bucketMinutes")
      } finally stmt.close()
    } finally conn.close()
  }
}
