package gg.boosted.dal

import com.datastax.driver.core.{BatchStatement, Cluster}
import gg.boosted.posos.SummonerChrole
import gg.boosted.{Champions, Role, Tier}
import org.slf4j.LoggerFactory

import collection.JavaConverters._

/**
  * Created by ilan on 8/28/16.
  */
object BoostedRepository {

    val log = LoggerFactory.getLogger(BoostedRepository.getClass)

    val cluster = Cluster.builder().addContactPoint("10.0.0.3").build()
    val session = cluster.connect("boostedgg")

    val insertPS = session.prepare("INSERT INTO MOST_BOOSTED_SUMMONERS_AT_CHROLES " +
            "(champion, role, winrate, summoner_id, summoner_name, region, rank, matches, position) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")

    val deletePS = session.prepare("DELETE FROM MOST_BOOSTED_SUMMONERS_AT_CHROLES where champion = ? and role = ? and position > ?")


    def truncateTable():Unit = {
        session.execute("TRUNCATE MOST_BOOSTED_SUMMONERS_AT_CHROLES")
    }


    def insertMatches(rows: Array[SummonerChrole]):Unit = {

        val batch = new BatchStatement()

        val champion = Champions.byId(rows(0).championId)
        val role = Role.byId(rows(0).roleId).toString

        for (position <- 0 to rows.length - 1) {
            val row = rows(position)
            val tier = Tier.byId(row.tier).toString
            batch.add(insertPS.bind(
                champion,
                role,
                Double.box(row.winrate),
                Long.box(row.summonerId),
                row.summonerName,
                row.region,
                tier,
                row.matches.asJava,
                Int.box(position + 1)))
        }
        batch.add(deletePS.bind(champion, role, Int.box(rows.length)))
        session.execute(batch)
        log.debug(s"Inserted ${rows.length} rows for $champion / $role")
    }
}
