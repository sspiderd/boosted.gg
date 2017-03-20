package gg.boosted;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Created by ilan on 3/20/17.
 */
public class CassandraStore {

    private static Cluster cluster = null;

    static {

        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .build();
            Session session = cluster.connect();                                           // (2)

            ResultSet rs = session.execute("select release_version from system.local");    // (3)
            Row row = rs.one();
            System.out.println(row.getString("release_version"));                          // (4)
        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }
    }

    public static void saveMatch(SummonerMatch sm) {
        Session session = cluster.connect();
        session.executeAsync(
                String.format("INSERT INTO SummonerMatches" +
                "VALUES (%d, %d, %d, %d, %d, %s, %d)",
                        sm.getChampionId(),
                        sm.getRoleId(),
                        sm.getMatchId(),
                        sm.getSummonerId(),
                        sm.getWinner(),
                        sm.getRegion(),
                        sm.getDate())
        );
    }

}
