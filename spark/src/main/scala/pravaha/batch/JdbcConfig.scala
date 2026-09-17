package pravaha.batch

import java.util.Properties

/** Postgres connection settings, read from the same POSTGRES_* environment
  * variable names Pravaha's own `.env.example` uses (see repo root README,
  * section "Environment variables"). This job reads from and writes to the
  * exact database Pravaha's API/workers already use - no separate datastore.
  */
final case class JdbcConfig(
    host: String,
    port: Int,
    db: String,
    user: String,
    password: String,
) {
  def url: String = s"jdbc:postgresql://$host:$port/$db"

  def properties: Properties = {
    val p = new Properties()
    p.setProperty("user", user)
    p.setProperty("password", password)
    p.setProperty("driver", "org.postgresql.Driver")
    p
  }
}

object JdbcConfig {
  def fromEnv(env: Map[String, String] = sys.env): JdbcConfig =
    JdbcConfig(
      host = env.getOrElse("POSTGRES_HOST", "localhost"),
      port = env.getOrElse("POSTGRES_PORT", "5432").toInt,
      db = env.getOrElse("POSTGRES_DB", "pravaha"),
      user = env.getOrElse("POSTGRES_USER", "pravaha"),
      password = env.getOrElse("POSTGRES_PASSWORD", "pravaha"),
    )
}
