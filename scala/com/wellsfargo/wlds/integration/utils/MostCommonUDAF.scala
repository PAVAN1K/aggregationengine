package com.wellsfargo.wlds.integration.utils

import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

case class MostCommonUDAF[T: TypeTag]() extends UserDefinedAggregateFunction {

  val datatype = schemaFor[T].dataType

  override def inputSchema: org.apache.spark.sql.types.StructType =
    new StructType()
      .add("value", datatype)

  override def bufferSchema: StructType =
    new StructType()
      .add("frequencyMap", DataTypes.createMapType(StringType, LongType))

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val existingMap = buffer.getAs[Map[String, Long]](0)
    if (!input.isNullAt(0) ) {
      val values:String=input.get(0).toString
      val inpString = values
      buffer(0) = existingMap + (if (existingMap.contains(inpString)) inpString -> (existingMap(inpString) + 1) else inpString -> 1L)
    }

  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1 = buffer1.getAs[Map[String, Long]](0)
    val map2 = buffer2.getAs[Map[String, Long]](0)
    buffer1(0) = map1 ++ map2.map { case (k, v) => k -> (v + map1.getOrElse(k, 0L)) }
  }

  override def evaluate(buffer: Row): Any = {
    val bufferinput=buffer.getAs[Map[String, Long]](0)
    if (!bufferinput.isEmpty){
      buffer.getAs[Map[String, Long]](0).toSeq.sortBy(cols=>cols._1).maxBy(_._2)._1
    }
    else{
      null
    }

  }
}


