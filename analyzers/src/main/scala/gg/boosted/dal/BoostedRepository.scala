package gg.boosted.dal

import com.datastax.driver.core.querybuilder.Truncate
import com.datastax.driver.core.{BatchStatement, Cluster, PreparedStatement, Statement}
import gg.boosted.{Champions, Role}
import gg.boosted.posos.SummonerChrole

/**
  * Created by ilan on 8/28/16.
  */
object BoostedRepository {

    val cluster = Cluster.builder().addContactPoint("10.0.0.3").build()
    val session = cluster.connect("boostedgg")

    def insertMatches(rows: Array[SummonerChrole]):Unit = {

//        val batch = new BatchStatement()
//        val ps = session.prepare("INSERT INTO MOST_BOOSTED_SUMMONERS_AT_CHROLES " +
//                "(champion, role, winrate, summonerId, region, rank, matches) " +
//                "VALUES (?, ?, ?, ?, ?, ?, ?)")
//        rows.foreach(row => {
//            val champion = Champions.byId(row.championId)
//            val role = Role.byId(row.roleId)
//            batch.add(ps.bind(champion, role, row.winrate, row.summonerId, row.region, row.tier, row.matches))
//        })
//        session.execute("TRUNCATE MOST_BOOSTED_SUMMONERS_AT_CHROLES")
//
//        session.execute(batch)

    }

}
