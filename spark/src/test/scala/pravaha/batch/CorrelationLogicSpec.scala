package pravaha.batch

import java.sql.Timestamp
import java.time.Instant
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Real correctness test of the correlation math, hand-computed and checked
  * against Spark's output.
  */
class CorrelationLogicSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("correlation-logic-spec")
      .master("local[2]")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.ansi.enabled", "false") // see EventCorrelationJob: NULL, not a thrown error, for zero variance
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def ts(s: String): Timestamp = Timestamp.from(Instant.parse(s))
  private def repeat(eventType: String, bucket: Timestamp, n: Int): Seq[Row] =
    Seq.fill(n)(Row(eventType, bucket))

  test("aggregate() computes exact Pearson correlation, including a dense zero-fill for a sparse type") {
    val b1 = ts("2026-09-17T10:00:00Z")
    val b2 = ts("2026-09-17T10:01:00Z")
    val b3 = ts("2026-09-17T10:02:00Z")
    val b4 = ts("2026-09-17T10:03:00Z")

    // A = [1,2,3,4], B = 2*A -> perfect +1 correlation
    // D = 5-A         -> perfect -1 correlation
    // C = [5,5,5,5]   -> zero variance -> correlation undefined (must be NULL, not 0)
    // E = [10,0,10,0] -> present in only 2 of 4 buckets; the other 2 MUST be
    //                    filled with an explicit zero by the dense grid, not
    //                    silently dropped (which would wrongly shrink n and
    //                    bias the statistic toward only co-occurring buckets)
    val rows =
      repeat("A", b1, 1) ++ repeat("A", b2, 2) ++ repeat("A", b3, 3) ++ repeat("A", b4, 4) ++
        repeat("B", b1, 2) ++ repeat("B", b2, 4) ++ repeat("B", b3, 6) ++ repeat("B", b4, 8) ++
        repeat("C", b1, 5) ++ repeat("C", b2, 5) ++ repeat("C", b3, 5) ++ repeat("C", b4, 5) ++
        repeat("D", b1, 4) ++ repeat("D", b2, 3) ++ repeat("D", b3, 2) ++ repeat("D", b4, 1) ++
        repeat("E", b1, 10) ++ repeat("E", b3, 10) // b2, b4 absent -> should densify to 0

    val input = spark.createDataFrame(spark.sparkContext.parallelize(rows), CorrelationLogic.InputSchema)

    val computedAt = Instant.parse("2026-09-17T12:00:00Z")
    val windowFrom = Instant.parse("2026-09-17T10:00:00Z")
    val windowTo = Instant.parse("2026-09-17T11:00:00Z")
    val out = CorrelationLogic.aggregate(input, bucketMinutes = 1, computedAt, windowFrom, windowTo).collect()

    // 5 event types -> C(5,2) = 10 unordered pairs, every one over the same 4 buckets
    assert(out.length == 10, s"expected 10 pairs, got ${out.length}")
    assert(out.forall(_.getAs[Long]("n_buckets") == 4L), "every pair must share the same dense n_buckets")
    assert(out.forall(_.getAs[Int]("bucket_minutes") == 1))
    assert(out.forall(_.getAs[String]("id").length == 36))
    assert(out.forall(_.getAs[Timestamp]("computed_at") == Timestamp.from(computedAt)))

    def pair(a: String, b: String) =
      out
        .find(r => r.getAs[String]("event_type_a") == a && r.getAs[String]("event_type_b") == b)
        .getOrElse(fail(s"missing pair ($a,$b) - check lexicographic ordering"))

    val ab = pair("A", "B")
    assert(ab.getAs[Double]("mean_count_a") === 2.5)
    assert(ab.getAs[Double]("mean_count_b") === 5.0)
    assert(math.abs(ab.getAs[Double]("pearson_r") - 1.0) < 1e-9, s"A/B should be perfectly correlated, got ${ab.getAs[Double]("pearson_r")}")

    val ad = pair("A", "D")
    assert(math.abs(ad.getAs[Double]("pearson_r") - (-1.0)) < 1e-9, s"A/D should be perfectly anti-correlated, got ${ad.getAs[Double]("pearson_r")}")

    val ac = pair("A", "C")
    assert(ac.isNullAt(ac.fieldIndex("pearson_r")), "zero-variance series must yield a NULL correlation, not a fabricated 0.0")

    val ae = pair("A", "E")
    assert(ae.getAs[Double]("mean_count_b") === 5.0, "E's dense mean must include the two zero-filled buckets: (10+0+10+0)/4")
    val expectedAE = -10.0 / math.sqrt(5.0 * 100.0) // hand-computed, see CorrelationLogicSpec comment
    assert(math.abs(ae.getAs[Double]("pearson_r") - expectedAE) < 1e-6, s"expected ~$expectedAE, got ${ae.getAs[Double]("pearson_r")}")
  }
}
