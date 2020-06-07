package com.minsait.telco.ioi.cdranalyzer

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector

/**
 * UDAF to add Vectors
 */
class VectorSum extends UserDefinedAggregateFunction {

  // Define the schema of the input data
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", VectorType) :: Nil)

  // Schema for internal fileds
  override def bufferSchema: StructType = StructType(
    StructField("sum", VectorType) :: Nil
  )

  // define the return type
  override def dataType: DataType = VectorType

  // Does the function return the same value for the same input?
  override def deterministic: Boolean = true

  // Initial values
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Vectors.zeros(0)
  }

  // Updated based on Input
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = vectorSum(buffer.getAs[Vector](0), input.getAs[Vector](0))
  }

  // Merge two schemas
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = vectorSum(buffer1.getAs[Vector](0), buffer2.getAs[Vector](0))
  }

  // Output
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Vector](0)
  }

  def vectorSum(a: Vector, b: Vector): Vector = {
    val aa = if (a.size > 0) a else Vectors.zeros(b.size)
    val bb = if (b.size > 0) b else Vectors.zeros(a.size)
    Vectors.dense((aa.toArray, bb.toArray).zipped.map(_ + _))
  }
}