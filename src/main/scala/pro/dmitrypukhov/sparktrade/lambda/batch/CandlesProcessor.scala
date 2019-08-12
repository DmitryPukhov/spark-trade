package pro.dmitrypukhov.sparktrade.lambda.batch

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.tuning.{CrossValidator, TrainValidationSplit}
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
    // For each value column replace value with diff from prev.
    // Use fold left
    columns./:[DataFrame](df)((df, colName: String) => {
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
    // Not sure it is better to use normalized diffs or just price/volume values
    val withDiff = toDiff(candles.toDF())


    // Todo: ... complete the code
  }
}
