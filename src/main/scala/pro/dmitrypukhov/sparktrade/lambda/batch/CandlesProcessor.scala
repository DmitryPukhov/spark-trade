package pro.dmitrypukhov.sparktrade.lambda.batch

//import org.apache.spark.ml.Pipeline
//import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}

import pro.dmitrypukhov.sparktrade.datamarts.prices.Candle
import pro.dmitrypukhov.sparktrade.storage.Lake
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions.{coalesce, datediff, lag, lit, min, sum, to_date}


/**
 * Calculate candles from raw data for Candle mart
 */
class CandlesProcessor extends BaseProcessor with Serializable {
  /**
   * Read raw data from lake, transform to Candle entity, save to candles table.
   */
  def process(): Unit = {
    // Read raw data from lake, transform to Candle entity, save to candles table.
    val candles = super.prepare[Candle](Lake.rawFinamCandlesDir + "/*.csv", Lake.candlesTableName, converter.asCandle, "candles")
    //val candles = super.prepare[Candle](Lake.rawFinamCandlesDir , Lake.candlesTableName, converter.asCandle, "candles")
    candles.show(false)
//    val timeWindow = Window.partitionBy("assetCode").orderBy("datetime")
//
//    Window.partitionBy("")
//    candles.groupBy("datetime").avg("close")
//    candles.groupBy(window("datetime","1 second","bb"))
//
//    val assembler = new VectorAssembler()
//      .setInputCols(Array("datetime","open", "high", "low", "close"))
//      .setOutputCol("")
////    val scaler = new StandardScaler()
////      .setInputCol(Array("open", "high", "low", "close"))
//    val pipeline = new Pipeline()

  }
}
