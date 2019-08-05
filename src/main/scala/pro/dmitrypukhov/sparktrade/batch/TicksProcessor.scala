package pro.dmitrypukhov.sparktrade.batch

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.apache.spark.sql.Row
import pro.dmitrypukhov.sparktrade.datamarts.prices.Tick
import pro.dmitrypukhov.sparktrade.storage.Lake

/**
 * Calculate ticks from raw data for Ticks mart
 */
class TicksProcessor extends PriceProcessor with Serializable {
  // Formatters are lazy to avoid NotSerializable exception
  private lazy val finamDateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  private lazy val finamTickFormatter = DateTimeFormatter.ofPattern("HHmmss")


  def process(): Unit =
    super.process[Tick](Lake.rawFinamTicksDir + "/*.csv", Lake.ticksTableName, asTick, "ticks")


  /**
   * Convert data row to tick entity
   * Finam data has specific date and time format, and I didn't find a way to autoconvert that.
   * Use hard coded names.
   */
  private def asTick(row: Row): Tick = {
    // Columns in csv: <TICKER>,<PER>,<DATE>,<TIME>,<LAST>,<VOL>
    // Compose date time from custom format
    val time = LocalTime.parse(row.getAs[String]("<TIME>"), finamTickFormatter)
    val date = LocalDate.parse(row.getAs[String]("<DATE>"), finamDateFormatter)
    val dt = LocalDateTime.of(date, time)

    // Construct the tick
    Tick(
      assetCode = row.getAs[String]("<TICKER>"),
      datetime = java.sql.Timestamp.valueOf(dt),
      price = row.getAs[String]("<LAST>").toDouble,
      vol = row.getAs[String]("<VOL>").toDouble)
  }
}
