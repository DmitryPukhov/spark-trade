package pro.dmitrypukhov.sparktrade.datamarts.prices

import org.apache.spark.sql.{Dataset, SparkSession}
import pro.dmitrypukhov.sparktrade.storage.Lake


/**
 * Data mart for OHLC and level1 ticks data consumers.
 */
class PriceMart extends Serializable {
  private val spark = SparkSession.active

  import spark.implicits._

  /**
   * Intraday candles of asset
   */
  def candles(assetCode: String, date: java.sql.Date): Dataset[Candle] =
    spark.table(Lake.candlesTableName)
      .where(s"assetCode = '$assetCode' AND date = '$date'")
      .as[Candle]

  /**
   * Intraday ticks of asset
   */
  def ticks(assetCode: String, date: java.sql.Date): Dataset[Tick] =
    spark.table(Lake.ticksTableName)
      .where(s"assetCode = '$assetCode' AND date = '$date'")
      .as[Tick]

}
