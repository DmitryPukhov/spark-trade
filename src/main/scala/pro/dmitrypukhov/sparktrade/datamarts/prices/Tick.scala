package pro.dmitrypukhov.sparktrade.datamarts.prices

/**
 * Tick entity: asset, time, price, vol
 */
case class Tick(assetCode: String, datetime: java.sql.Timestamp, price: Double, vol: Double)
