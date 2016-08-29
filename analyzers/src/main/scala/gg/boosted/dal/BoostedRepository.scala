package gg.boosted.dal

import com.datastax.driver.core.{BatchStatement, Cluster}
import gg.boosted.posos.SummonerChrole
import gg.boosted.{Champions, Role, Tier}
import collection.JavaConverters._

/**
  * Created by ilan on 8/28/16.
  */
object BoostedRepository {

    val cluster = Cluster.builder().addContactPoint("10.0.0.3").build()
    val session = cluster.connect("boostedgg")

    def insertMatches(rows: Array[SummonerChrole]):Unit = {

        val batch = new BatchStatement()
        val ps = session.prepare("INSERT INTO MOST_BOOSTED_SUMMONERS_AT_CHROLES " +
                "(champion, role, winrate, summonerId, region, rank, matches, position) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")

        val champion = Champions.byId(rows(0).championId)
        val role = Role.byId(rows(0).roleId).toString

        for (position <- 0 to rows.length - 1) {
            val row = rows(position)
            val tier = Tier.byId(row.tier).toString
            batch.add(ps.bind(
                champion,
                role,
                Double.box(row.winrate),
                Long.box(row.summonerId),
                row.region,
                tier,
                row.matches.asJava,
                Int.box(position + 1)))
        }
        val delete = session.prepare(s"DELETE FROM MOST_BOOSTED_SUMMONERS_AT_CHROLES where champion = ? and role = ? and position > ?")
        batch.add(delete.bind(champion, role, Int.box(rows.length)))
        session.execute(batch)

    }

}
