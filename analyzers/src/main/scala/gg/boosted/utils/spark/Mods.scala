package gg.boosted.utils.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by ilan on 2/8/17.
  */
//Extend UserDefinedAggregateFunction to write custom aggregate function
//You can also specify any constructor arguments. For instance you
//can have CustomMean(arg1: Int, arg2: String)
class Mods extends UserDefinedAggregateFunction {

  def inputSchema: StructType =
    StructType(StructField("value", LongType, false) :: Nil)

  def bufferSchema: StructType =
    StructType(StructField("list", ArrayType(LongType, true), true) :: Nil)

  override def dataType: DataType = ArrayType(LongType, true)

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = IndexedSeq[Long]()
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (buffer != null) {
      val seq = buffer(0).asInstanceOf[IndexedSeq[Long]]
      buffer(0) = input.getAs[Long](0) +: seq

    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer1(0) != null && buffer2 != null) {
      val seq1 = buffer1(0).asInstanceOf[IndexedSeq[Long]]
      val seq2 = buffer2(0).asInstanceOf[IndexedSeq[Long]]
      buffer1(0) = seq1 ++ seq2
    }
  }

  def evaluate(buffer: Row): Any = {
    if (buffer(0) == null) {
      IndexedSeq[Long]()
    } else {
      buffer(0).asInstanceOf[IndexedSeq[Long]]
    }
  }

}


