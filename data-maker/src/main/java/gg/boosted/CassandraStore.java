package gg.boosted;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import gg.boosted.configuration.Configuration;
import groovy.transform.CompileStatic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * Created by ilan on 3/20/17.
 */
@CompileStatic
public class CassandraStore {

    static Logger log = LoggerFactory.getLogger(CassandraStore.class);

    private static Cluster cluster = null;
    private static Session session = null;

    static {

            cluster = Cluster.builder()
                    .addContactPoint(Configuration.getString("cassandra.location"))
                    .build();
            session = cluster.connect();
    }

    public static void saveMatch(SummonerMatch sm) {
        ResultSetFuture future = session.executeAsync(
                String.format("INSERT INTO BoostedGG.SUMMONER_MATCHES " +
                        "(champion_id," +
                                "  role_id," +
                                "  creation_date," +
                                "  match_id," +
                                "  summoner_id," +
                                "  winner," +
                                "  platform_id," +
                                "  patch_major_version," +
                                "  patch_minor_version) " +
                "VALUES (%d, %d, %s, %d, %d, %d, %s, %d, %d)",
                        sm.getChampionId(),
                        sm.getRole().roleId,
                        sm.getCreationDate(),
                        sm.getGameId(),
                        sm.getSummonerId(),
                        sm.getWinner(),
                        "'" + sm.getPlatformId() + "'",
                        sm.getPatchMajorVersion(),
                        sm.getPatchMinorVersion())
        );
        Futures.addCallback(future,
                new FutureCallback<ResultSet>() {
                    @Override public void onSuccess(ResultSet result) {
                        log.debug("Success inserting to cassandra");
                    }

                    @Override public void onFailure(Throwable t) {
                        log.error("", t);
                    }
                },
                MoreExecutors.directExecutor()
        );
    }

}
