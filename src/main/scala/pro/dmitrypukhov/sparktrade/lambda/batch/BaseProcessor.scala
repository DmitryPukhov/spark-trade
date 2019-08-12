package pro.dmitrypukhov.sparktrade.lambda.batch

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import pro.dmitrypukhov.sparktrade.lambda.FinamEntityConverter

/**
 * Common logic for ticks and candles price processing
 */
trait BaseProcessor extends LazyLogging {
  protected val spark: SparkSession = SparkSession.active
  protected val converter: FinamEntityConverter = new FinamEntityConverter()

  import spark.implicits._

  /**
   * Read raw data from lake, transform to Candle or tick entity, save to given table.
   */
  protected def prepare[EntityType <: Product : TypeTag](rawSrc: String, tableName: String, convert: Row => EntityType, tag: String): Dataset[EntityType] = {

    logger.info(s"Batch layer. Transforming raw $tag from $rawSrc to $tableName.")
    // Transform row finam candles to Candle entities
    val transformedDs = spark.read
      .option("header", "true")
      .csv(rawSrc)
      .map(convert)
      .withColumn("date", to_date($"datetime"))
      .orderBy("datetime")

    // Save to candles table, partitioned by asset and time
    logger.info(s"Writing $tag to $tableName")
    transformedDs.write.mode(SaveMode.Overwrite)
      .partitionBy("assetCode", "date")
      .saveAsTable(tableName)

    logger.info(s"Batch layer. Transformed raw $tag from $rawSrc to $tableName.")

    // Return transformed entities
    spark.read.table(tableName).as[EntityType]
  }
}
