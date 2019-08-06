package pro.dmitrypukhov.sparktrade.batch

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.apache.spark.sql.Row
import pro.dmitrypukhov.sparktrade.datamarts.prices.Candle
import pro.dmitrypukhov.sparktrade.storage.Lake

/**
 * Calculate candles from raw data for Candle mart
 */
class CandlesProcessor extends BaseProcessor with Serializable {
  // Formatters are lazy to avoid NotSerializable exception
  private lazy val finamDateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  /**
   * Read raw data from lake, transform to Candle entity, save to candles table.
   */
  def process(): Unit =  super.process[Candle](Lake.rawFinamCandlesDir + "/*.csv", Lake.candlesTableName, asCandle, "candles")

  /**
   * Convert data row to candle entity
   * Finam data has specific date and time format, and I didn't find a way to autoconvert that.
   * Use hard coded names.
   */
  private def asCandle(row: Row): Candle = {
    // Compose date time from custom format
    val time = LocalTime.parse(row.getAs[String]("<TIME>"))
    val date = LocalDate.parse(row.getAs[String]("<DATE>"), finamDateFormatter)
    val dt = LocalDateTime.of(date, time)
    // Construct the candle
    Candle(
      assetCode = row.getAs[String]("<TICKER>"),
      datetime = java.sql.Timestamp.valueOf(dt),
      open = row.getAs[String]("<OPEN>").toDouble,
      high = row.getAs[String]("<HIGH>").toDouble,
      low = row.getAs[String]("<LOW>").toDouble,
      close = row.getAs[String]("<CLOSE>").toDouble,
      vol = row.getAs[String]("<VOL>").toDouble)
  }
}
