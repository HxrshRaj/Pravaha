package pravaha.batch

import java.sql.Timestamp
import java.time.Instant
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Real correctness test of the aggregation math, computed by hand and
  * checked against Spark's output - not "it ran without throwing."
  */
class RollupLogicSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("rollup-logic-spec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def ts(s: String): Timestamp = Timestamp.from(Instant.parse(s))

  test("aggregate() computes exact counts, sums, distincts and late/replay flags") {
    val hourA = ts("2026-09-17T10:00:00Z")
    val hourB = ts("2026-09-17T11:00:00Z")

    // Hand-picked so every output number can be verified by inspection:
    //   payment.completed / us-east / hourA: 3 rows
    //     corr: c1, c1, c2            -> 2 distinct correlation ids
    //     user: u1, u2, u2            -> 2 distinct users
    //     amount: 100.0, 50.0, null   -> total 150.0, avg over 2 non-null = 75.0
    //     late: 1, 0, 0               -> late_event_count = 1
    //     replay: 0, 0, 1             -> replay_event_count = 1
    //   order.created / eu-west / hourA: 1 row (different group -> separate output row)
    //   payment.completed / us-east / hourB: 1 row (different hour -> separate output row)
    val rows = Seq(
      Row("payment.completed", "us-east", hourA, "c1", "u1", 100.0, 1, 0),
      Row("payment.completed", "us-east", hourA, "c1", "u2", 50.0, 0, 0),
      Row("payment.completed", "us-east", hourA, "c2", "u2", null, 0, 1),
      Row("order.created", "eu-west", hourA, "c3", "u3", 200.0, 0, 0),
      Row("payment.completed", "us-east", hourB, "c4", "u4", 10.0, 0, 0),
    )
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      RollupLogic.InputSchema,
    )

    val computedAt = Instant.parse("2026-09-17T12:00:00Z")
    val windowFrom = Instant.parse("2026-09-17T10:00:00Z")
    val windowTo = Instant.parse("2026-09-17T12:00:00Z")
    val out = RollupLogic.aggregate(input, computedAt, windowFrom, windowTo).collect()

    assert(out.length == 3, "one output row per distinct (event_type, region, hour_bucket)")

    val pcHourA = out
      .find(r => r.getAs[String]("event_type") == "payment.completed" && r.getAs[Timestamp]("hour_bucket") == hourA)
      .getOrElse(fail("missing payment.completed/us-east/hourA group"))

    assert(pcHourA.getAs[String]("region") == "us-east")
    assert(pcHourA.getAs[Long]("event_count") == 3L)
    assert(pcHourA.getAs[Long]("distinct_correlation_ids") == 2L)
    assert(pcHourA.getAs[Long]("distinct_users") == 2L)
    assert(pcHourA.getAs[Double]("total_amount") == 150.0)
    assert(pcHourA.getAs[Double]("avg_amount") == 75.0)
    assert(pcHourA.getAs[Long]("late_event_count") == 1L)
    assert(pcHourA.getAs[Long]("replay_event_count") == 1L)
    assert(pcHourA.getAs[Timestamp]("window_from") == Timestamp.from(windowFrom))
    assert(pcHourA.getAs[Timestamp]("window_to") == Timestamp.from(windowTo))
    assert(pcHourA.getAs[Timestamp]("computed_at") == Timestamp.from(computedAt))
    assert(pcHourA.getAs[String]("id").length == 36, "id should be a UUID string")

    val oc = out
      .find(_.getAs[String]("event_type") == "order.created")
      .getOrElse(fail("missing order.created/eu-west/hourA group"))
    assert(oc.getAs[String]("region") == "eu-west")
    assert(oc.getAs[Long]("event_count") == 1L)
    assert(oc.getAs[Double]("total_amount") == 200.0)

    val pcHourB = out
      .find(r => r.getAs[String]("event_type") == "payment.completed" && r.getAs[Timestamp]("hour_bucket") == hourB)
      .getOrElse(fail("missing payment.completed/us-east/hourB group"))
    assert(pcHourB.getAs[Long]("event_count") == 1L)
    assert(pcHourB.getAs[Double]("total_amount") == 10.0)
    assert(pcHourB.getAs[Long]("late_event_count") == 0L)
  }

  test("aggregate() handles an all-null-amount group without crashing (coalesces to 0.0)") {
    val hour = ts("2026-09-17T10:00:00Z")
    val rows = Seq(
      Row("user.login", "unknown", hour, "c1", "u1", null, 0, 0),
      Row("user.login", "unknown", hour, "c2", "u2", null, 0, 0),
    )
    val input = spark.createDataFrame(spark.sparkContext.parallelize(rows), RollupLogic.InputSchema)
    val out = RollupLogic
      .aggregate(input, Instant.now(), Instant.parse("2026-09-17T10:00:00Z"), Instant.parse("2026-09-17T11:00:00Z"))
      .collect()

    assert(out.length == 1)
    assert(out(0).getAs[Long]("event_count") == 2L)
    assert(out(0).getAs[Double]("total_amount") == 0.0)
    assert(out(0).getAs[Double]("avg_amount") == 0.0)
  }
}
