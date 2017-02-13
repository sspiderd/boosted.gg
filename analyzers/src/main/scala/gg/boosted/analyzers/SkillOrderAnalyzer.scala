package gg.boosted.analyzers

import gg.boosted.Application
import org.apache.spark.sql.{Dataset, Row}
import gg.boosted.Application.session.implicits._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
  * Created by ilan on 2/13/17.
  */
object SkillOrderAnalyzer {

  case class SkillOrderSetup(skillOrder: Seq[Int], winner:Boolean)

  case class SkillLevelWinner(skill:Int, level:Int, winner:Boolean)

  def skillOrderSetupToSkillLevelWinner(skillOrderDataset:Dataset[SkillOrderSetup]):Dataset[SkillLevelWinner] = {
    skillOrderDataset.flatMap(row => {
      row.skillOrder.zipWithIndex.map {case (skill, i) => SkillLevelWinner(skill, i+1, row.winner)}
    })
  }

  /**
    *
    * I'm gonna try it gal's way..
    * Make a bucket for each level. for each level calculate the skill that gave the highest winrate
    * And put it in the bucket. if the skill was already used up (used 5 of it), discard it and take the runnerup
    * Until we fill all the buckets
    *
    * @param skillOrderDataset
    * @return
    */
  def optimalSkillOrder(skillOrderDataset:Dataset[SkillOrderSetup]):Seq[Int] = {
    //This is basically an aggregation on each column separately
    //So first i'm gonna flatmap this into (skill, level, winner) triplets
    val skillLevelWinner = skillOrderSetupToSkillLevelWinner(skillOrderDataset)
    val defPar = Application.session.sparkContext.defaultParallelism
    val repartitioned = skillLevelWinner.repartition(defPar, $"level").cache()
    val bestSkillPerLevel = Seq[Int]()
    val numberOfTimesASkillWasUsed = Map(1->0, 2->0, 3->0, 4->0)
    val maxedSkills = Seq[Int]()
    //TODO: In the unlikely event that there are no 18 skills, i should fill it up
    val maxLevel:Int = repartitioned.agg(max("level")).collect()(0)(0).asInstanceOf[Int]
    for (i <- 1 to maxLevel) {
      println(repartitioned
        .filter(_.level == i)
        .filter(slw => !maxedSkills.contains(slw.skill))
        .groupBy("skill")
        .mean("winner")
        .orderBy(desc("winner"))
        .head())

    }
    return null;
  }


}
