package pro.dmitrypukhov.sparktrade.lambda.speed

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import pro.dmitrypukhov.sparktrade.datamarts.prices.Candle

/**
 * Speed layer. Processing candles.
 */
class CandlesStream {
  protected val log: Logger = LoggerFactory.getLogger(this.getClass)
  protected val config: Config = ConfigFactory.load()
  protected val spark: SparkSession = SparkSession.active

  import spark.implicits._

  def process(df: DataFrame): Unit = {
    df.as[Candle]

  }
}
