package com.usecase.sql

import com.usecase.AppUseCase.logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, concat, count, lit, rank, round, row_number, sum}

object Transformation {

  def addLiteralColumn(data: DataFrame): DataFrame = {
    data.select(col("*"), lit(1).alias("literal"))
  }

  def createTop(data: DataFrame): DataFrame  = {

    val windowSpecNP = Window.orderBy("literal")
    val windowOrdered = Window.orderBy(col("records").desc)

    val dfRecords = data
      .withColumn("total_records", count("literal").over(windowSpecNP))
      .select("noc", "total_records")

    val dfRank = data.groupBy("noc")
      .agg(count("noc").as("records"))
      .withColumn("rank",row_number().over(windowOrdered))

    val recordsWithRank = dfRank.alias("rank").join(
      broadcast(dfRecords.alias("records")),
      dfRank("noc") === dfRecords("noc"),
      "inner"
    )
    recordsWithRank.dropDuplicates().select("records.noc","records","total_records","rank")
  }

  def tableFiltered(data: DataFrame): DataFrame ={

    data.where(col("noc") === "Japan")

  }

  def createPercent(data: DataFrame):DataFrame={
    data.withColumn("percent_records",round((col ("records")/col("total_records"))*100,2))
  }

  def createMessage(data: DataFrame):DataFrame={
    data.withColumn("message", concat(lit("Japan in place: "), col("rank"),lit(" with "),col("total"),lit(" won medals!")))
  }


  def joinObjects(data1: DataFrame,data2: DataFrame,data3: DataFrame):DataFrame={
    val joinedTables = data1.join(data2,"noc").join(data3,"noc")

    joinedTables.select(
      data1("noc").as("noc"),
      data1("records").as("athletes_records"),
      data1("total_records").as("athletes_total_records"),
      data1("rank").as("athletes_rank"),
      data1("percent_records").as("athletes_percent_records"),
      data2("records").as("coaches_records"),
      data2("total_records").as("coaches_total_records"),
      data2("rank").as("coaches_rank"),
      data2("percent_records").as("coaches_percent_records"),
      data3("gold").as("gold"),
      data3("silver"),
      data3("bronze"),
      data3("total").as("total_medals"),
      data3("rank_by_total"),
      data3("rank"),
      data3("message")
    )


  }
}
