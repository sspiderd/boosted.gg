package gg.boosted.dal

import java.util
import java.util.Date

import com.datastax.driver.core.{BatchStatement, Cluster}
import gg.boosted.{Role, Tier}
import org.slf4j.LoggerFactory

/**
  * Created by ilan on 8/28/16.
  */
object BoostedRepository {

    val log = LoggerFactory.getLogger(BoostedRepository.getClass)

    val BOOSTED_TABLE_NAME = "BOOSTED_SUMMONERS_BY_CHAMPION_AND_ROLE"

    val cluster = Cluster.builder().addContactPoint("10.0.0.3").build()
    val session = cluster.connect("boostedgg")

    val insertPS = session.prepare(s"INSERT INTO $BOOSTED_TABLE_NAME " +
            "(champion, role, winrate, summoner_id, summoner_name, region, tier, division, league_points, lol_score, matches, rank, last_updated) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

    val deletePS = session.prepare(s"DELETE FROM $BOOSTED_TABLE_NAME " +
        "where champion = ? and role = ? and rank > ?")

    def truncateTable():Unit = {
        session.execute(s"TRUNCATE $BOOSTED_TABLE_NAME")
    }

    def insertMatches(rows: Array[BoostedEntity], timestamp:Date):Unit = {

        import collection.JavaConverters._

        //Partition by chrole
        val partitionedByChrole = rows.groupBy(row => (row.champion,row.role))


        truncateTable()

        partitionedByChrole.values.foreach ( partition => {
            val batch = new BatchStatement()
            partition.foreach(row => {
                batch.add(insertPS.bind(
                    row.champion,
                    row.role,
                    Double.box(row.winrate),
                    Long.box(row.summonerId),
                    row.summonerName,
                    row.region.toString,
                    row.tier,
                    row.division,
                    Int.box(row.leaguePoints),
                    Int.box(row.lolScore),
                    row.matches.asJava,
                    Int.box(row.rank),
                    timestamp)
                )
            })
            session.execute(batch)
        })

        log.debug(s"Updated $BOOSTED_TABLE_NAME table")
    }
}
