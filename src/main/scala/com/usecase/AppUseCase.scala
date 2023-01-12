package com.usecase

import com.usecase.sql.Transformation
import com.usecase.utils.AppSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, avg, col, count, dense_rank, desc, max, min, rank, row_number, sum}

object AppUseCase extends AppSession {

  def run(): Unit = {

    /**
     * INPUTS
     */
    logger.info("=====> Reading file")

    val dfAthletes = readParquet("input.pathAthletes")
    val dfCoaches = readParquet("input.pathCoaches")
    val dfMedals = readParquet("input.pathMedals")


    /**
     * TRANSFORMATIONS
     */
    logger.info("=====> Transforming data")

    //Ejercicio1
    val dfAthletesWithExtraColumn = Transformation.addLiteralColumn(dfAthletes)
    //dfAthletesWithExtraColumn.show()
    val dfCoachesWithExtraColumn = Transformation.addLiteralColumn(dfCoaches)
    dfCoachesWithExtraColumn.show()

    val topAthletes = Transformation.createTop(dfAthletesWithExtraColumn)
    topAthletes.show()
    val topCoaches = Transformation.createTop(dfCoachesWithExtraColumn)
    topCoaches.show()
    //Ejercicio2
    val athletesFiltered = Transformation.tableFiltered(topAthletes)
    athletesFiltered.show()
    val coachesFiltered = Transformation.tableFiltered(topCoaches)
    coachesFiltered.show()
    val medalsFiltered = Transformation.tableFiltered(dfMedals)
    medalsFiltered.show()
    //Ejercicio3
    val athletesResume = Transformation.createPercent(athletesFiltered)
    athletesResume.show()
    val coachesResume = Transformation.createPercent(coachesFiltered)
    coachesResume.show(false)
    //Ejercicio4
    val messageResume = Transformation.createMessage(medalsFiltered)
    messageResume.show(false)
    //Ejercicio5
    val joinedTables = Transformation.joinObjects(athletesResume,coachesResume,messageResume)
    joinedTables.show(false)

    /**
     * OUTPUT
     */


    //dfAthletesWithExtraColumn.printSchema() //Imprime el esquema de la bdd
    //dfAthletesWithExtraColumn.show
//    joinedTables.write
//      .partitionBy("noc")
//      .parquet("output.path")
    spark.stop()
    logger.info("=====> end process")

  }
}
