package gg.boosted.posos

import gg.boosted.Role
import gg.boosted.maps.{Champions, Items}
import org.apache.spark.sql.Dataset

/**
  * Created by ilan on 1/3/17.
  */
case class Mindset(championId:Int, roleId:Int, cluster: Int, coreItems:Seq[Int], winrate: Double, gamesPlayed:Long, summonerMatches:Seq[String])

case class MindSetExplained(champion: String, role: String, cluster: Int, coreItems:Seq[String], winrate: Double, gamesPlayed: Long)

object Mindset {

    def explain(mindset:Dataset[Mindset]):Dataset[MindSetExplained] = {
        import gg.boosted.Application.session.implicits._
        mindset.map(row => MindSetExplained(
            Champions.byId(row.championId),
            Role.byId(row.roleId).toString,
            row.cluster,
            row.coreItems.map(Items.byId(_).name),
            row.winrate,
            row.gamesPlayed
        ))
    }
}
