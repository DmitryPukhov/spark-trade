package pro.dmitrypukhov.sparktrade.lambda.batch

import org.apache.spark.ml.feature.{Normalizer, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lag, _}
import pro.dmitrypukhov.sparktrade.datamarts.prices.Candle
import pro.dmitrypukhov.sparktrade.storage.Lake


/**
 * Calculate candles from raw data for Candle mart
 */
class CandlesProcessor extends BaseProcessor with Serializable {

  /**
   * Transform raw data to candles
   */
  def updateCandlesFromRaw(): Dataset[Candle] = super.prepare[Candle](Lake.rawFinamCandlesDir + "/*.csv", Lake.candlesTableName, converter.asCandle, "candles")

  /**
   * Add future values and diff values
   */
  private def withDiff(df: DataFrame) = {
    // Value columns
    val columns = Array("open", "high", "low", "close", "vol")
    val timeWindow = Window
      .partitionBy("assetCode")
      .orderBy("datetime")
    val lagSize = 1
    val futLagSize = -1
    // For each value column replace value with diff from prev.
    // Use fold left
    columns./:[DataFrame](df)((df, colName: String) => {
      val diffName = s"${colName}_diff"
      // Next value column for prediction
      val futDiffName = s"${colName}_fut_diff"
      df.withColumn(diffName, col(colName) - lag(colName, lagSize).over(timeWindow))
        //df.withColumn(diffName, col(colName) - lag(colName, lagSize).over(timeWindow))
        .withColumn(futDiffName, lag(colName, futLagSize).over(timeWindow) - col(colName))
        .drop(colName)
    })
      // Drop nans
      .na.drop()

  }

  /**
   * Features preparation stages for pipeline
   *
   * @return
   */
  private val featuresStages: Array[PipelineStage] = Array[PipelineStage](
    // Encode asset with one hot encoder
    new StringIndexer()
      .setInputCol("assetCode")
      .setOutputCol("assetCodeIndex"),

    // Combine ohlc to features vector
    new VectorAssembler()
      .setInputCols(Array("open_diff", "high_diff", "low_diff", "close_diff", "vol_diff"))
      .setOutputCol("features_assembled"),

    // Normalize ohlcv columns
    new StandardScaler()
      .setInputCol("features_assembled")
      .setOutputCol("features_scaled"),

    new Normalizer()
      .setInputCol("features_scaled")
      .setOutputCol("features"),
  )

  /**
   * Whole batch process.
   */
  def process(): Unit = {
    updateCandlesFromRaw()
    predict()
  }


  /**
   * Read raw data from lake, transform to Candle entity, save to candles table.
   * todo: complete
   */
  def predict(): Unit = {

    // Read candles dataset
    var candles = spark.read.table(Lake.candlesTableName)
    // Convert values to diff from previous value
    // Not sure it is better to use normalized diffs or just price/volume values
    candles = withDiff(candles)
    candles.show(false)

    // Split to train and test
    val cnt = candles.count()
    val testRatio = 0.3
    val train = candles.orderBy("datetime").limit(Math.abs(cnt.toInt - (cnt * testRatio).toInt))

    // Build predictor, let it be Linear Regression
    val predictor = new RandomForestRegressor()
      .setFeaturesCol("features")
      .setLabelCol("close_fut_diff")
      //.setMaxIter(10)

    // Create pipeline with preparation and predictor
    val pipeline = new Pipeline()
      .setStages(featuresStages :+ predictor)


    // Create pipeline with predictor and train
    val model = pipeline
      .fit(train)

    // Predict
    val predictions = model.transform(train)
    predictions.select("datetime", "close_diff", "close_fut_diff", "prediction").show(false)

    // ToDo: continue
  }
}
