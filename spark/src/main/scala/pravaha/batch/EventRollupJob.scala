package pravaha.batch

import java.sql.{DriverManager, Timestamp}
import java.time.Instant
import org.apache.spark.sql.{SaveMode, SparkSession}

/** Pravaha batch layer: hourly event rollups.
  *
  * Reads a time window of rows from the `events` table (Pravaha's real,
  * durable event store - populated by the persistence consumer, see
  * pravaha/analytics/persistence.py), aggregates them per
  * (event_type, region, hour), and writes the result into
  * `batch_event_rollups` (schema owned by the `BatchEventRollup` SQLAlchemy
  * model + its Alembic migration, so the Python API can read it too).
  *
  * Idempotent: reruns for the same window first delete any rollup rows whose
  * hour_bucket falls inside `[from, to)`, then insert fresh ones - safe to
  * replay a window as many times as you like, same as Pravaha's own replay
  * engine is safe to rerun.
  *
  * Usage (from spark/):
  *   sbt "run --hours 24"
  *   sbt "run --from 2026-09-17T00:00:00Z --to 2026-09-17T06:00:00Z"
  */
object EventRollupJob {

  private val TargetTable = "batch_event_rollups"

  def main(args: Array[String]): Unit = {
    val jobArgs = JobArgs.parse(args)
    val jdbc = JdbcConfig.fromEnv()

    println(s"[pravaha-batch] window: ${jobArgs.from} -> ${jobArgs.to}")
    println(s"[pravaha-batch] source: ${jdbc.url} (table: events)")

    val spark = SparkSession
      .builder()
      .appName("pravaha-event-rollup")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.ui.showConsoleProgress", "false")
      // Spark's JDBC read/write path converts timestamps through the SESSION
      // timezone, not a fixed UTC instant, unless told otherwise - on a host
      // whose JVM default zone isn't UTC (found the hard way: hour_bucket came
      // back shifted by the local UTC offset on this IST machine) that silently
      // corrupts every timestamp round-tripped through JDBC. Force UTC so what
      // Postgres calls '2026-09-17 09:00:00+00' round-trips as exactly that.
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      val raw = readEvents(spark, jdbc, jobArgs)
      val rawCount = raw.count()
      println(s"[pravaha-batch] read $rawCount event(s) from the window")

      val rollups = RollupLogic.aggregate(raw, Instant.now(), jobArgs.from, jobArgs.to)
      val rollupCount = rollups.count()
      println(s"[pravaha-batch] computed $rollupCount rollup row(s)")

      rollups.orderBy("hour_bucket", "event_type", "region").show(50, truncate = false)

      deleteWindow(jdbc, jobArgs)
      writeRollups(rollups, jdbc)
      println(s"[pravaha-batch] wrote $rollupCount row(s) to $TargetTable")
    } finally {
      spark.stop()
    }
  }

  /** Pull the exact columns RollupLogic needs. The JSON extraction
    * (`payload->>'user_id'`) and the UTC hour truncation are pushed down into
    * Postgres via the subquery, so Spark's JDBC reader only ever sees plain
    * scalar column types over the wire (JSONB itself does not map cleanly
    * through Spark's JDBC dialect).
    */
  private def readEvents(spark: SparkSession, jdbc: JdbcConfig, a: JobArgs) = {
    val fromLit = Timestamp.from(a.from).toInstant.toString
    val toLit = Timestamp.from(a.to).toInstant.toString

    val subquery =
      s"""(
         |  SELECT
         |    event_type,
         |    COALESCE(region, 'unknown')                                AS region,
         |    date_trunc('hour', timezone('UTC', event_time))            AS hour_bucket,
         |    correlation_id,
         |    payload ->> 'user_id'                                      AS user_id,
         |    amount,
         |    -- SQLAlchemy's Enum(..., native_enum=False) persists the Python
         |    -- enum MEMBER NAME ('ON_TIME'/'ACCEPTED_LATE'/'TOO_LATE'), not its
         |    -- .value ('on_time'/...) - confirmed against the live column, not
         |    -- assumed (see README "Batch layer" section, "a real bug this
         |    -- caught"). Compare against the stored form, not the enum value.
         |    CASE WHEN lateness_at_ingest <> 'ON_TIME' THEN 1 ELSE 0 END AS is_late,
         |    CASE WHEN is_replay THEN 1 ELSE 0 END                      AS is_replay
         |  FROM events
         |  WHERE event_time >= TIMESTAMPTZ '$fromLit'
         |    AND event_time <  TIMESTAMPTZ '$toLit'
         |) AS windowed_events""".stripMargin

    spark.read
      .jdbc(jdbc.url, subquery, jdbc.properties)
  }

  /** Clear any prior rollup rows for the hour buckets this run touches, so a
    * rerun of the same window overwrites rather than duplicates.
    */
  private def deleteWindow(jdbc: JdbcConfig, a: JobArgs): Unit = {
    Class.forName("org.postgresql.Driver")
    val conn = DriverManager.getConnection(jdbc.url, jdbc.user, jdbc.password)
    try {
      val stmt = conn.prepareStatement(
        s"DELETE FROM $TargetTable " +
          "WHERE hour_bucket >= date_trunc('hour', timezone('UTC', ?::timestamptz)) " +
          "AND hour_bucket < ?::timestamptz"
      )
      try {
        stmt.setString(1, a.from.toString)
        stmt.setString(2, a.to.toString)
        val deleted = stmt.executeUpdate()
        println(s"[pravaha-batch] cleared $deleted existing rollup row(s) for this window")
      } finally stmt.close()
    } finally conn.close()
  }

  private def writeRollups(rollups: org.apache.spark.sql.DataFrame, jdbc: JdbcConfig): Unit =
    rollups.write
      .mode(SaveMode.Append)
      .jdbc(jdbc.url, TargetTable, jdbc.properties)
}
