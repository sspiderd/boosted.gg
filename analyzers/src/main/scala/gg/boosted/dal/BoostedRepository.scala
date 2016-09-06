package gg.boosted.dal

import java.util
import java.util.Date

import com.datastax.driver.core.{BatchStatement, Cluster}
import gg.boosted.{Champions, Role, Tier}
import org.slf4j.LoggerFactory

/**
  * Created by ilan on 8/28/16.
  */
object BoostedRepository {

    val log = LoggerFactory.getLogger(BoostedRepository.getClass)

    val cluster = Cluster.builder().addContactPoint("10.0.0.3").build()
    val session = cluster.connect("boostedgg")

    val insertPS = session.prepare("INSERT INTO MOST_BOOSTED_SUMMONERS_AT_CHROLES " +
            "(champion, role, winrate, summoner_id, summoner_name, region, tier, matches, rank, last_updated) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

    val deletePS = session.prepare("DELETE FROM MOST_BOOSTED_SUMMONERS_AT_CHROLES where champion = ? and role = ? and rank > ?")

    def truncateTable():Unit = {
        session.execute("TRUNCATE MOST_BOOSTED_SUMMONERS_AT_CHROLES")
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
                    row.region,
                    row.tier,
                    row.matches.asJava,
                    Int.box(row.rank),
                    timestamp)
                )
            })
            session.execute(batch)
        })

        log.debug(s"Updated MOST_BOOSTED_SUMMONERS_AT_CHROLES table")
    }
}
