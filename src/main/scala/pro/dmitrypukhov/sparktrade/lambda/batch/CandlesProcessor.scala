package pro.dmitrypukhov.sparktrade.lambda.batch

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Predictor}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, TrainValidationSplit}
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
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
   * Add future values and diff values
   */
  private def withDiff(df: DataFrame) = {
    // Value columns
    val columns = Array("open", "high", "low", "close", "vol")
    df.show(false)
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
      val nextName = s"${colName}_fut"
      df.withColumn(diffName, col(colName) - lag(colName, lagSize).over(timeWindow))
        .withColumn(nextName, lag(colName, futLagSize).over(timeWindow))
    })
  }

  /**
   * Create pipeline for preparation steps
   *
   * @return
   */
  private def pipelineOf(predictor: PipelineStage): Pipeline = {
    // Encode asset with one hot encoder
    val assetEncoder = new StringIndexer()
      .setInputCol("assetCode")
      .setOutputCol("assetCodeIndex")

    // Combine ohlc to features vector
    val featuresAssembler = new VectorAssembler()
      .setInputCols(Array("open", "high", "low", "close", "vol"))
      .setOutputCol("ohlcv_features")

    // Normalize ohlcv columns
    val featuresScaler = new StandardScaler()
      .setInputCol("ohlcv_features")
      .setOutputCol("features")

//    // Combine labels to labels vector
//    val labelsAssembler = new VectorAssembler()
//      .setInputCols(Array("open_fut", "high_fut", "low_fut", "close_fut", "vol_fut"))
//      .setOutputCol("ohlcv_labels")


    // Normalize ohlcv labels columns
    val labelsScaler = new StandardScaler()
      .setInputCol("close_fut")
      .setOutputCol("labels")

    // Build pipeline for preparation stages
    val pipeline = new Pipeline()
      .setStages(Array(assetEncoder,
        featuresAssembler,
        featuresScaler,
        //labelsAssembler,
        //labelsScaler,
        predictor))

    pipeline
  }

  def prepare() = super.prepare[Candle](Lake.rawFinamCandlesDir + "/*.csv", Lake.candlesTableName, converter.asCandle, "candles")
  /**
   * Read raw data from lake, transform to Candle entity, save to candles table.
   * todo: complete
   */
  def process(): Unit = {
    // Transform raw data to candles
    prepare()

    // Read candles dataset
    var candles = spark.read.table(Lake.candlesTableName)
    // Convert values to diff from previous value
    // Not sure it is better to use normalized diffs or just price/volume values
    candles = withDiff(candles)
    candles.show(false)

    // Split to train and test
    val cnt = candles.count()
    val testRatio = 0.3
    val test = candles.orderBy(desc("datetime")).limit((cnt * testRatio).toInt)
    val train = candles.orderBy("datetime").limit(Math.abs(cnt.toInt - (cnt * testRatio).toInt))

    // Build predictor, let it be Linear Regression
    val predictor = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("close_fut")
      .setMaxIter(10)

    // Create pipeline with predictor and train
    val model = pipelineOf(predictor)
      .fit(train)

    // Predict
    val predictions = model.transform(train)
    predictions.show(false)

    // ToDo: continue
  }
}
