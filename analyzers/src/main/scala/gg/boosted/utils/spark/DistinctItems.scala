package gg.boosted.utils.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
/**
  * Created by ilan on 2/8/17.
  */
class DistinctItems extends UserDefinedAggregateFunction {

  // Input Data Type Schema
  def inputSchema: StructType = StructType(Array(StructField("item", ArrayType(StringType))))

  // Intermediate Schema
  def bufferSchema = StructType(Array(
    StructField("total", ArrayType(StringType))
  ))

  // Returned Data Type .
  def dataType: DataType = ArrayType(StringType, false)

  // Self-explaining
  def deterministic = true

  // This function is called whenever key changes
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = Seq[String]()
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    val seq = buffer(0).asInstanceOf[Seq[String]]
    val input1 = input.getAs[Seq[String]](0)
    buffer(0) =  input1 ++ seq
  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    val seq1 = buffer1(0).asInstanceOf[Seq[String]]
    val seq2 = buffer2(0).asInstanceOf[Seq[String]]
    buffer1(0) = seq1 ++ seq2
  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row) = {
    buffer(0).asInstanceOf[Seq[String]].distinct
  }

}
