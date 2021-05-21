package com.wellsfargo.wlds.integration.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._
/** *
 * Mode UDAF or MostCommon UDAF. It is Typed Version.
 */
case class WeightedAverageUDAF[T:TypeTag](implicit n:Numeric[T]) extends UserDefinedAggregateFunction{

  val valueDataType:DataType=schemaFor[T].dataType

  override def inputSchema: org.apache.spark.sql.types.StructType =
    new StructType()
      .add("value",StringType ,true)
      .add("weight", StringType,true)

  override def bufferSchema: StructType =
    new StructType()
      .add("productWeigthValue", DoubleType,true)
      .add("sumWeight", DoubleType,true)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit ={
    buffer.update(0,n.zero)
    buffer.update(1,n.zero)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//    if(!(input.isNullAt(0) | input.isNullAt(1))) {
//      val values:Double=input.get(0) match{
//        case l:String=>l.toDouble
//        case l:Integer=>l.toDouble
//        case l:java.lang.Double=>l
//        case l:java.lang.Long=>l.toDouble
//        case null=>0.toDouble
//      }
//      val weights:Double=input.get(1) match {
//        case l: String => l.toDouble
//        case l: Integer => l.toDouble
//        case l: java.lang.Double => l
//        case l: java.lang.Long => l.toDouble
//        case null=>0.toDouble
//      }

//      buffer(0)=buffer.getDouble(0) + (values * weights)
//      buffer(1)=buffer.getDouble(1) + weights

    buffer(0)=n.plus(buffer.getAs[T](0),n.times(input.getAs[T](0),input.getAs[T](1)))
    buffer(1)=n.plus(buffer.getAs[T](1),input.getAs[T](0))

    }


  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

//    buffer1(0)=(buffer1.getDouble(0) * buffer1.getDouble(1)) +(buffer2.getDouble(0) * buffer2.getDouble(1))
//    buffer1(1)=buffer1.getDouble(1) + buffer2.getDouble(1)
    buffer1(0)=n.plus(buffer1.getAs[T](0),buffer2.getAs[T](0))
    buffer1(1)=n.plus(buffer1.getAs[T](1),buffer2.getAs[T](1))
  }

  override def evaluate(buffer: Row): Any = {
    val output=n.toDouble(buffer.getAs[T](0)) /n.toDouble(buffer.getAs[T](1))
    if(output.isNaN){
      return null
    }
    output
  }
}


