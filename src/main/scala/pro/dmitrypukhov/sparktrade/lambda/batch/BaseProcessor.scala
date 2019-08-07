package pro.dmitrypukhov.sparktrade.lambda.batch

import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import pro.dmitrypukhov.sparktrade.lambda.FinamEntityConverter

/**
 * Common logic for ticks and candles price processing
 */
trait BaseProcessor {
  protected val spark: SparkSession = SparkSession.active
  protected val log: Logger = LoggerFactory.getLogger(this.getClass)
  protected val converter:FinamEntityConverter = new FinamEntityConverter()

  import spark.implicits._

  /**
   * Read raw data from lake, transform to Candle or tick entity, save to given table.
   */
  protected def process[EntityType <: Product : TypeTag](rawSrc: String, tableName: String, convert: Row => EntityType, tag: String): Unit = {

    log.info(s"Batch layer. Transforming raw $tag from $rawSrc to $tableName.")
    // Transform row finam candles to Candle entities
    val transformedDs = spark.read
      .option("header", "true")
      .csv(rawSrc)
      .map(convert)
      .withColumn("date", to_date($"datetime"))

    // Save to candles table, partitioned by asset and time
    log.info(s"Writing $tag to $tableName")
    transformedDs.write.mode(SaveMode.Overwrite)
      .partitionBy("assetCode", "date")
      .saveAsTable(tableName)

    log.info(s"Batch layer. Transformed raw $tag from $rawSrc to $tableName.")
  }
}
