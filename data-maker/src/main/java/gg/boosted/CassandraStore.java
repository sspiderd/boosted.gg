package gg.boosted;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;
import gg.boosted.configuration.Configuration;
import groovy.transform.CompileStatic;

/**
 * Created by ilan on 3/20/17.
 */
@CompileStatic
public class CassandraStore {

    private static Cluster cluster = null;
    private static Session session = null;

    static {

            cluster = Cluster.builder()
                    .addContactPoint(Configuration.getString("cassandra.location"))
                    .build();
            session = cluster.connect();
    }

    public static void saveMatch(SummonerMatch sm) {
        Session session = cluster.connect();
        session.executeAsync(
                String.format("INSERT INTO SummonerMatches " +
                "VALUES (%d, %d, %d, %d, %d, %s, %d)",
                        sm.getChampionId(),
                        sm.getRoleId(),
                        sm.getMatchId(),
                        sm.getSummonerId(),
                        sm.getWinner(),
                        sm.getRegion(),
                        sm.getCreationDate())
        );
    }

}
