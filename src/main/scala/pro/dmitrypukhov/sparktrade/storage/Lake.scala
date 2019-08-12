package pro.dmitrypukhov.sparktrade.storage

import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

/**
 *
 * Storage layer. Data lake configuration: locations, tables.
 */
object Lake extends Serializable with LazyLogging {
  private val spark = SparkSession.active
  private lazy val config = ConfigFactory.load()
  private val lakeRootDir = config.getString("sparktrade.storage.lake.root.dir")
  val dbName: String = "sparktrade"

  /**
   * Raw ticks in datalake, imported from Finam provider
   */
  val rawFinamTicksDir: String = Paths.get(lakeRootDir, "files/raw/finam/ticks").toString

  /**
   * Raw candles in datalake, imported from Finam provider
   */
  val rawFinamCandlesDir: String = Paths.get(lakeRootDir, "files/raw/finam/candles").toString

  /**
   * All OHLC data contains in candles table, partitioned by asset and day
   */
  val candlesTableName = s"$dbName.candles"

  /**
   * Time, vol and price level1 ticks
   */
  val ticksTableName = s"$dbName.ticks"

  /**
   * Create initial datalake if started from scratch
   */
  def init(): Unit = {
    logger.info("Init data lake")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
  }

}
