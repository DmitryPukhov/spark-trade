package pro.dmitrypukhov.sparktrade.lambda.batch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lag, _}
import pro.dmitrypukhov.sparktrade.datamarts.prices.Candle
import pro.dmitrypukhov.sparktrade.storage.Lake


/**
 * Calculate candles from raw data for Candle mart
 */
class CandlesProcessor extends BaseProcessor with Serializable {
  /**
   * Time window size in bars
   */
  val windowSize = 5

  /**
   * For value columns add delta from previous value
   */
  private def toDiff(df: DataFrame) = {
    // Value columns
    val columns = Seq("open", "high", "low", "close", "vol")

    val timeWindow = Window
      .partitionBy("assetCode")
      .orderBy("datetime")
    val lagSize = 1
    columns.foldLeft[DataFrame](df)((df, colName: String) => {
      // Replace value column with diff from prev value
      val tmpName = s"${colName}_diff_tmp"
      df.withColumn(tmpName, col(colName) - lag(colName, lagSize).over(timeWindow))
        .drop(colName)
        .withColumnRenamed(tmpName, colName)
    })
  }

  /**
   * Read raw data from lake, transform to Candle entity, save to candles table.
   */
  def process(): Unit = {
    // Transform raw data to candles
    val candles = super.prepare[Candle](Lake.rawFinamCandlesDir + "/*.csv", Lake.candlesTableName, converter.asCandle, "candles")
    // Convert values to diff from previous value
    val withDiff = toDiff(candles.toDF())
    withDiff.show(false)
    // Todo: ... complete the code
  }
}
