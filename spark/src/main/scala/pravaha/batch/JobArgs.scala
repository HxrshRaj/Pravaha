package pravaha.batch

import java.time.{Duration, Instant}

/** The batch window this run recomputes: `[from, to)`, both UTC instants.
  *
  * CLI usage:
  *   --to <ISO-8601 instant>     default: now
  *   --hours <N>                 default: 24 (ignored if --from given)
  *   --from <ISO-8601 instant>   default: to - hours
  */
final case class JobArgs(from: Instant, to: Instant) {
  require(from.isBefore(to), s"--from ($from) must be strictly before --to ($to)")
}

object JobArgs {
  def parse(args: Array[String]): JobArgs = {
    val opts: Map[String, String] =
      args
        .sliding(2, 2)
        .collect { case Array(k, v) if k.startsWith("--") => k.drop(2) -> v }
        .toMap

    val to = opts.get("to").map(parseInstant("--to", _)).getOrElse(Instant.now())
    val hours = opts.get("hours").map(_.toLong).getOrElse(24L)
    val from = opts
      .get("from")
      .map(parseInstant("--from", _))
      .getOrElse(to.minus(Duration.ofHours(hours)))

    JobArgs(from, to)
  }

  private def parseInstant(flag: String, raw: String): Instant =
    try Instant.parse(raw)
    catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          s"$flag must be an ISO-8601 instant like 2026-09-17T00:00:00Z, got '$raw'",
          e,
        )
    }
}
