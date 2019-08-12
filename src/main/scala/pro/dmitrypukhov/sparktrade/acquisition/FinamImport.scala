package pro.dmitrypukhov.sparktrade.acquisition

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{SaveMode, SparkSession}
import pro.dmitrypukhov.sparktrade.storage.Lake


/**
 * Acquisition layer. Reads the data from external csv file provided by Finam.
 * For development purpose. Production version should load from network.
 **/
class FinamImport extends LazyLogging with Serializable {
  private val spark = SparkSession.active
  private val config = ConfigFactory.load()

  /**
   * Import raw finam candles to the lake
   */
  def importCandles(): Unit = {
    val src = config.getString("sparktrade.acquisition.finam.candles.src")
    val dstDir = Lake.rawFinamCandlesDir

    importAny(src, dstDir, "candles")
  }

  /**
   * Import raw finam ticks to the lake
   */
  def importTicks(): Unit = {
    val src = config.getString("sparktrade.acquisition.finam.ticks.src")
    importAny(src, Lake.rawFinamTicksDir, "ticks")
  }

  private def importAny(src: String, dstDir: String, tag: String): Unit = {
    logger.info(s"Acquisition layer: $tag. Importing data from $src")
    // Read from input
    val df = spark.read
      .option("header", "true")
      .csv(src)

    // Write to lake preserving raw format
    logger.info(s"Acquisition layer:$tag. Reading from source $src to data lake $dstDir")
    // Csv contains data in name, so no lost existing data, overwrite the whole day
    df.write.mode(SaveMode.Overwrite).option("header", "true").csv(dstDir)
    logger.info(s"Acquisition layer. Imported from $src to $dstDir")
  }
}
