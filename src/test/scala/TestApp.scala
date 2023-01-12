import com.typesafe.config.Config
import com.usecase.utils.LoadConf
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, lit, max}
import org.scalatest.funsuite.AnyFunSuite

import java.util

class TestApp extends AnyFunSuite {


  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("example")
    .getOrCreate()



  val dfAthletes = spark.read.parquet("src/test/resources/data/input/t_fdev_athletes")
  val dfCoaches = spark.read.parquet("src/test/resources/data/input/t_fdev_coaches")
  val dfMedals = spark.read.parquet("src/test/resources/data/input/t_fdev_medals")

  test("Ejercicio1 ") {


    val dfwc = dfAthletes.select(col("*"), lit(1).alias("literal"))
    val windowSpecNP = Window.orderBy("literal")


    val max = dfwc.withColumn("total_records", count("literal").over(windowSpecNP))
      .select("total_records").first().getAs[Long](0)

    val conteo = dfwc.count()

    assert(max === conteo)

  }

  test("Ejercicio2"){

  }


}
