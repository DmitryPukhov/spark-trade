package pro.dmitrypukhov.sparktrade.lambda

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.apache.spark.sql.Row
import pro.dmitrypukhov.sparktrade.datamarts.prices.{Candle, Tick}

/**
 * Converts from raw format to Candles and Ticks models
 */
class FinamEntityConverter extends Serializable {
  // Date and time in Finam csv are of different formats
  // Formatters are lazy to avoid NotSerializable exception
  private lazy val candlesDateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  private lazy val ticksDateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  private lazy val ticksTimeFormatter = DateTimeFormatter.ofPattern("HHmmss")

  /**
   * Convert data row to candle entity
   * Finam data has specific date and time format, and I didn't find a way to autoconvert that.
   * Use hard coded names.
   */
  def asCandle(row: Row): Candle = {
    // Compose date time from custom format
    val time = LocalTime.parse(row.getAs[String]("<TIME>"))
    val date = LocalDate.parse(row.getAs[String]("<DATE>"), candlesDateFormatter)
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

  /**
   * Convert data row to tick entity
   * Finam data has specific date and time format, and I didn't find a way to autoconvert that.
   * Use hard coded names.
   */
  def asTick(row: Row): Tick = {
    // Columns in csv: <TICKER>,<PER>,<DATE>,<TIME>,<LAST>,<VOL>
    // Compose date time from custom format
    val time = LocalTime.parse(row.getAs[String]("<TIME>"), ticksTimeFormatter)
    val date = LocalDate.parse(row.getAs[String]("<DATE>"), ticksDateFormatter)
    val dt = LocalDateTime.of(date, time)

    // Construct the tick
    Tick(
      assetCode = row.getAs[String]("<TICKER>"),
      datetime = java.sql.Timestamp.valueOf(dt),
      price = row.getAs[String]("<LAST>").toDouble,
      vol = row.getAs[String]("<VOL>").toDouble)
  }
}
