package gg.boosted.riotapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import gg.boosted.riotapi.constants.QueueType;
import gg.boosted.riotapi.dtos.*;
import gg.boosted.riotapi.dtos.match.MatchDetail;
import gg.boosted.riotapi.throttlers.DistributedThrottler;
import gg.boosted.riotapi.throttlers.IThrottler;
import gg.boosted.riotapi.utilities.ArrayChunker;
import gg.boosted.riotapi.utilities.ArrayConverter;
import gg.boosted.riotapi.utilities.Matches;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by ilan on 12/9/16.
 */
public class RiotApi {

    private static Logger log = LoggerFactory.getLogger(RiotApi.class) ;

    private Region region ;
    private String riotApiKey ;
    private String regionEndpoint;
    private String staticEndpoint;
    Client client = ClientBuilder.newClient() ;
    ObjectMapper om = new ObjectMapper() ;
    private IThrottler throttler ;

    public RiotApi(Region region) {
        this.region = region ;
        riotApiKey = System.getenv("RIOT_API_KEY") ;
        if (riotApiKey == null) {
            throw new RuntimeException("You need to set environment variable \"RIOT_API_KEY\" with your riot api key") ;
        }
        regionEndpoint = "https://" +
                region.toString().toLowerCase() +
                ".api.pvp.net/api/lol/" +
                region.toString().toLowerCase() ;

        staticEndpoint = "https://global.api.pvp.net/api/lol/static-data/" + region.toString().toLowerCase() ;
        throttler = new DistributedThrottler(10, 500, region) ;
    }

    private String callApiJson(String endpoint) {

        //Add the riotApi key to the endpoint
        if (endpoint.contains("?")) {
            endpoint += "&";
        } else {
            endpoint += "?";
        }
        endpoint += "api_key=" + riotApiKey;

        WebTarget target = client.target(endpoint) ;

        //I'm trying to shave off a few ms by taking into account that the roundtrip itself takes some time to finish
        //I think a good rough estimate is shaving off half the round trip.. we'll see...
        long beforeApiCall, roundTrip = 0;

        boolean statikk = false ;
        if (endpoint.startsWith(staticEndpoint)) {
            statikk = true;
        }

        //Don't stop believing
        while (true) {
            try {
                //If it's a static endpoint, we don't need a throttler since we're allowed endless calls
                if (!statikk) {
                    throttler.waitFor();
                }
                log.debug("API Called: {}", endpoint);
                beforeApiCall = System.currentTimeMillis() ;
                String response = target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
                roundTrip = System.currentTimeMillis() - beforeApiCall;
                log.debug("Roundtrip {}", roundTrip);
                return response ;

            } catch (ClientErrorException cer) {
                int status = cer.getResponse().getStatus() ;
                String error = String.format("Bad status: {%d} -> {%s}", status, cer.getResponse().getStatusInfo().getReasonPhrase());
                if (status == 429) {
                    error = error.concat(" : rateLimitCount {" + cer.getResponse().getHeaderString("X-Rate-Limit-Count") + "}") ;
                    String retryAfter = cer.getResponse().getHeaderString("Retry-After") ;
                    if (retryAfter != null) {
                        error = error.concat(" : retryAfter {" + retryAfter + "}") ;
                        long retryMillis = Long.parseLong(retryAfter) * 1000 ;
                        log.warn("retryAfter header sent. will retry afterApiCall {} millis", retryMillis);
                        try {
                            Thread.sleep(retryMillis);
                        } catch (InterruptedException e) {
                            log.error("I shouldn't really be here");
                        }
                    }
                } else if (status == 404) {
                    log.error("GOT 404 from server!");
                    return null;
                }
                log.error(error);
            } catch (Exception ex) {
                log.error("Logged unknown error", ex) ;
                //throw new RuntimeException(ex) ;
            } finally {
                if (!statikk) {
                    throttler.releaseLock(System.currentTimeMillis() - roundTrip);
                }
            }
        }
    }

    private JsonNode callApi(String endpoint) {
        try {
            String json = callApiJson(endpoint);
            if (json == null) {
                return NullNode.getInstance();
            }
            return om.readValue(json, JsonNode.class);
        } catch (IOException ex) {
            log.error("Logged unknown error", ex) ;
        }
        throw new RuntimeException("How did i get here??");
    }

    public List<Champion> getChampionsList() throws IOException {
        String endpoint = staticEndpoint + "/v1.2/champion";

        JsonNode rootNode = callApi(endpoint) ;

        List<Champion> champions = new LinkedList<>() ;
        //Riot has made this api a little silly, so i need to be silly to get the data
        Iterator<Map.Entry<String, JsonNode>> it = rootNode.get("data").fields() ;
        while (it.hasNext()) {
            JsonNode node = it.next().getValue() ;
            champions.add(om.treeToValue(node, Champion.class)) ;
        }
        return champions ;
    }

    public List<MatchReference> getMatchList(long summonerId, long beginTime) throws IOException {
        String QUEUES = "RANKED_SOLO_5x5,TEAM_BUILDER_RANKED_SOLO" ;
        String endpoint = regionEndpoint + "/v2.2/matchlist/by-summoner/" + String.valueOf(summonerId) +
                "?rankedQueues=" + QUEUES +
                "&beginTime=" + beginTime ;

        JsonNode rootNode = callApi(endpoint) ;

        return om.readValue(rootNode.get("matches").toString(), new TypeReference<List<MatchReference>>(){}) ;
    }

    //public List<String> getChallengerIds

    /**
     * Get the features games, just return a json string
     * @return
     */
    public String getFeaturedGames() throws IOException {
        String endpoint = "https://" + region.toString().toLowerCase() +
                ".api.pvp.net/observer-mode/rest/featured" ;
        return callApiJson(endpoint) ;
    }

    public Map<String, Long> getSummonerIdsByNames(String... names) throws IOException {
        Map<String, Long> map = new HashMap<>() ;
        //We can get 40 names for each call, so
        int chunkSize = 40;
        List<String[]> chunkedArray = ArrayChunker.split(names, chunkSize) ;

        for (String[] array : chunkedArray) {
            String endpoint = regionEndpoint +
                    "/v1.4/summoner/by-name/" ;
            endpoint += Arrays.stream(array).collect(Collectors.joining(",")) ;
            endpoint += "?api_key=" + riotApiKey ;
            JsonNode rootNode = callApi(endpoint) ;
            //Riot has made this api a little silly, so i need to be silly to get the data
            Iterator<Map.Entry<String, JsonNode>> it = rootNode.fields() ;
            while (it.hasNext()) {
                JsonNode node = it.next().getValue() ;
                map.put(node.get("name").asText(), node.get("id").asLong()) ;
            }
        }
        return map ;
    }

    public Map<Long, String> getSummonerNamesByIds(Long... ids) {
        Map<Long, String> map = new HashMap<>() ;
        List<Long[]> chunkedArray = ArrayChunker.split(ids, 40) ;
        for (Long[] array : chunkedArray) {

            String endpoint = String.format("%s/v1.4/summoner/", regionEndpoint) ;
            endpoint += Arrays.stream(ArrayConverter.convertLongToString(array)).collect(Collectors.joining(",")) ;
            JsonNode rootNode = callApi(endpoint) ;
            //Riot has made this api a little silly, so i need to be silly to get the data
            Iterator<Map.Entry<String, JsonNode>> it = rootNode.fields() ;
            while (it.hasNext()) {
                JsonNode node = it.next().getValue() ;
                map.put(node.get("id").asLong(), node.get("name").asText()) ;
            }
        }
        return map;
    }

    public List<Long> getChallengersIds() {
        List<Long> challengerIds = new LinkedList<>() ;
        String endpoint = String.format("%s/v2.5/league/challenger?type=%s", regionEndpoint, QueueType.RANKED_SOLO_5x5) ;
        JsonNode root = callApi(endpoint) ;
        Iterator<JsonNode> it = root.get("entries").elements() ;
        while (it.hasNext()) {
            JsonNode node = it.next() ;
            challengerIds.add(node.get("playerOrTeamId").asLong());
        }
        return challengerIds ;
    }

    public List<Long> getMastersIds() {
        List<Long> challengerIds = new LinkedList<>() ;
        String endpoint = String.format("%s/v2.5/league/master?type=%s", regionEndpoint, QueueType.RANKED_SOLO_5x5) ;
        JsonNode root = callApi(endpoint) ;
        Iterator<JsonNode> it = root.get("entries").elements() ;
        while (it.hasNext()) {
            JsonNode node = it.next() ;
            challengerIds.add(node.get("playerOrTeamId").asLong());
        }
        return challengerIds ;
    }

    public String getMatchAsJson(long matchId, boolean includeTimeline) {
        String endpoint = String.format("%s/v2.2/match/%s?includeTimeline=%s", regionEndpoint, matchId, includeTimeline) ;
        return callApiJson(endpoint);
    }

    public MatchDetail getMatch(long matchId, boolean includeTimeline) {
        String endpoint = String.format("%s/v2.2/match/%s?includeTimeline=%s", regionEndpoint, matchId, includeTimeline) ;
        JsonNode root = callApi(endpoint);
        try {
            return om.treeToValue(root, MatchDetail.class) ;
        } catch (JsonProcessingException e) {
            log.error("Processing exception", e);
            throw new RuntimeException(e) ;
        }
    }

    public Map<Long, LeagueEntry> getLeagueEntries(Long... summonerIds) {
        Map<Long, LeagueEntry> map = new HashMap<>() ;

        Set<Long> found = new HashSet<>();
        Set<Long> all = new HashSet<>() ;

        int chunkSize = 10;
        List<Long[]> chunkedArray = ArrayChunker.split(summonerIds, chunkSize) ;
        for (Long[] array : chunkedArray) {
            String endpoint = regionEndpoint +
                    "/v2.5/league/by-summoner/" ;
            endpoint += Arrays.stream(ArrayConverter.convertLongToString(array)).collect(Collectors.joining(",")) ;
            JsonNode rootNode = callApi(endpoint) ;

            //Riot has made this api a little silly, so i need to be silly to get the data
            Iterator<Map.Entry<String, JsonNode>> it = rootNode.fields() ;
            while (it.hasNext()) {
                JsonNode node = it.next().getValue().elements().next() ;
                JsonNode entry = node.get("entries").elements().next();
                try {
                    LeagueEntry leagueEntry = om.treeToValue(entry, LeagueEntry.class) ;
                    leagueEntry.tier = node.get("tier").asText();
                    Long summonerId = Long.parseLong(leagueEntry.playerOrTeamId) ;
                    map.put(summonerId, leagueEntry) ;
                    found.add(summonerId) ;
                } catch (JsonProcessingException e) {
                    log.error("Processing exception", e);
                    throw new RuntimeException(e) ;
                }
            }
        }

        //For all of those not found (this means that they are unranked) add an unranked league entry
        all.removeAll(found) ;
        for (Long unknown : all) {
            LeagueEntry unranked = new LeagueEntry();
            unranked.playerOrTeamId = String.valueOf(unknown);
            unranked.tier = "UNRANKED";
            unranked.division = "U";
            map.put(unknown, unranked) ;

        }

        return map ;
    }

    public MatchSummary getMatchSummary(long matchId) {
        return Matches.detailToSummary(getMatch(matchId, true));
    }

    public String getMatchSummaryAsJson(long matchId) {
        try {
            return om.writeValueAsString(getMatchSummary(matchId));
        } catch (JsonProcessingException e) {
            log.error("Shouldn't be thrown");
        }
        throw new RuntimeException("Shouldn't be thrown");
    }

    /**
     * I'm really only interested in the gold, so i'm not bringing everything back
     * @return
     */
    public Map<Integer, Item> getItems() {
        Map<Integer, Item>itemsList = new HashMap<>() ;
        String endpoint = String.format("%s/v1.2/item?itemListData=gold", staticEndpoint) ;
        JsonNode root = callApi(endpoint) ;
        Iterator<JsonNode> it = root.get("data").elements() ;
        while (it.hasNext()) {
            JsonNode node = it.next() ;
            Object name = node.get("name");
            if (name != null) {
                Item item = new Item();
                item.id = node.get("id").asInt();
                item.name = node.get("name").asText();
                item.gold = node.get("gold").get("total").asInt();
                itemsList.put(item.id, item);
            }
        }
        return itemsList ;

    }


    public static void main(String[] args) throws IOException {
        //new RiotApi(Region.EUW).getChampionsList() ;
        //new RiotApi(Region.KR).getMatchList(2035958L, 1) ;
//        for (String s : new RiotApi(Region.EUW).getMastersIds()) {
//            System.out.println(s);
//        }
//        for (String s : new RiotApi(Region.EUW).getChallengersIds()) {
//            System.out.println(s);
//        }

        //new RiotApi(Region.EUW).getMatch(2969769203L, false) ;
        new RiotApi(Region.EUW).getItems();
    }


}
