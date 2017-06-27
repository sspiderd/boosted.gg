package gg.boosted.riotapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import gg.boosted.riotapi.constants.QueueType;
import gg.boosted.riotapi.dtos.*;
import gg.boosted.riotapi.dtos.match.Match;
import gg.boosted.riotapi.dtos.match.MatchReference;
import gg.boosted.riotapi.dtos.match.MatchTimeline;
import gg.boosted.riotapi.throttlers.DistributedThrottler;
import gg.boosted.riotapi.throttlers.IThrottler;
import gg.boosted.riotapi.throttlers.SimpleThrottler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.*;

/**
 * Created by ilan on 12/9/16.
 */
public class RiotApi {

    private static Logger log = LoggerFactory.getLogger(RiotApi.class) ;

    private Platform platform;
    private String riotApiKey ;
    private String regionEndpoint;
    private String staticEndpoint;
    Client client = ClientBuilder.newClient() ;
    ObjectMapper om = new ObjectMapper() ;
    IThrottler throttler ;

    public RiotApi(Platform platform) {
        this.platform = platform;
        riotApiKey = System.getenv("RIOT_API_KEY") ;
        if (riotApiKey == null) {
            throw new RuntimeException("You need to set environment variable \"RIOT_API_KEY\" with your riot api key") ;
        }
        regionEndpoint = "https://" +
                platform.toString().toLowerCase() +
                ".api.riotgames.com/lol" ;

        staticEndpoint = "https://" +
                platform.toString().toLowerCase() +
                ".api.riotgames.com/lol/static-data/v3" ;
        throttler = new DistributedThrottler(10, 500, platform) ;
    }

    private String callApiJson(String endpoint) {

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
                String response = target
                        .request(MediaType.APPLICATION_JSON_TYPE)
                        .header("X-Riot-Token", riotApiKey)
                        .get(String.class);
                roundTrip = System.currentTimeMillis() - beforeApiCall;
                log.trace("Roundtrip {}", roundTrip);
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

    private <T> T callApi(String endpoint, Class<T> clazz)  {
        JsonNode node = callApi(endpoint) ;
        if (node instanceof NullNode) return null ;
        try {
            return om.treeToValue(node, clazz) ;
        } catch (JsonProcessingException e) {
            log.error("Processing exception", e);
            throw new RuntimeException(e) ;
        }
    }

    public List<Champion> getChampionsList() throws IOException {
        String endpoint = staticEndpoint + "/champions";

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

    public Summoner getSummoner(long summonerId) throws IOException {
        String endpoint = regionEndpoint + "/summoner/v3/summoners/" + String.valueOf(summonerId) ;
        return callApi(endpoint, Summoner.class) ;
    }

    public List<MatchReference> getMatchList(long accountId, long beginTime) throws IOException {
        String QUEUE = "420" ;
        String endpoint = regionEndpoint + "/match/v3/matchlists/by-account/"
                + String.valueOf(accountId) +
                "?queue=" + QUEUE +
                "&beginTime=" + beginTime ;

        JsonNode rootNode = callApi(endpoint) ;

        if (rootNode.get("matches") == null) return new LinkedList<>() ;

        return om.readValue(rootNode.get("matches").toString(), new TypeReference<List<MatchReference>>(){}) ;
    }

    //public List<String> getChallengerIds

    /**
     * Get the features games, just return a json string
     * @return
     */
    public String getFeaturedGames() throws IOException {
        String endpoint = regionEndpoint + "/spectator/v3/featured-games" ;
        return callApiJson(endpoint) ;
    }

    public Summoner getSummonerByName(String name) throws IOException {
        String endpoint = regionEndpoint + "/summoner/v3/summoners/by-name/" + name ;
        return callApi(endpoint, Summoner.class) ;
    }


    public List<Long> getChallengersIds() {
        List<Long> challengerIds = new LinkedList<>() ;
        String endpoint = String.format("%s/league/v3/challengerleagues/by-queue/%s", regionEndpoint, QueueType.RANKED_SOLO_5x5) ;
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
        String endpoint = String.format("%s/league/v3/masterleagues/by-queue/%s", regionEndpoint, QueueType.RANKED_SOLO_5x5) ;
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

    public Match getMatch(long matchId) {
        String endpoint = String.format("%s/match/v3/matches/%s", regionEndpoint, matchId) ;
        return callApi(endpoint, Match.class);
    }

    public MatchTimeline getMatchTimeline(long matchId) {
        String endpoint = String.format("%s/match/v3/timelines/by-match/%s", regionEndpoint, matchId) ;
        return callApi(endpoint, MatchTimeline.class);
    }

    public LeaguePosition getLeaguePosition(Long summonerId) throws JsonProcessingException {
        String endpoint = regionEndpoint + "/league/v3/positions/by-summoner/" + summonerId ;
        JsonNode rootNode = callApi(endpoint);
        Iterator<JsonNode> it = rootNode.elements() ;
        while (it.hasNext()) {
            JsonNode node = it.next();
            if (node.get("queueType").asText().equals("RANKED_SOLO_5x5")) {
                return om.treeToValue(node, LeaguePosition.class) ;
            }
        }
        return null ;
    }


    /**
     * I'm really only interested in the gold, so i'm not bringing everything back
     * @return
     */
    public Map<String, Item> getItems() {
        Map<String, Item>itemsList = new HashMap<>() ;
        String endpoint = String.format("%s/v1.2/item?itemListData=gold,stats", staticEndpoint) ;
        JsonNode root = callApi(endpoint) ;
        Iterator<JsonNode> it = root.get("data").elements() ;
        while (it.hasNext()) {
            JsonNode node = it.next() ;
            Object name = node.get("name");
            if (name != null) {
                try {
                    Item item = new Item();
                    item.id = node.get("id").asInt();
                    item.name = node.get("name").asText();
                    item.gold = node.get("gold").get("total").asInt();
                    item.stats = om.readValue(node.get("stats").toString(), Stats.class);
                    itemsList.put(item.id.toString(), item);
                } catch (IOException e) {
                    throw new RuntimeException("Should'nt have happened") ;
                }

            }
        }
        return itemsList ;
    }

    public Map<String, RuneDef> getRuneDefs() {
        Map<String, RuneDef> runeList = new HashMap<>();

        String endpoint = String.format("%s/v1.2/rune?runeListData=stats", staticEndpoint) ;
        JsonNode root = callApi(endpoint) ;
        Iterator<JsonNode> it = root.get("data").elements() ;
        while (it.hasNext()) {
            JsonNode node = it.next() ;
            try {
                RuneDef rune = new RuneDef();
                rune.id = node.get("id").asInt();
                rune.name = node.get("name").asText();
                rune.tier = node.get("rune").get("tier").asInt();
                rune.type = node.get("rune").get("type").asText();
                rune.stats = om.readValue(node.get("stats").toString(), Stats.class);
                runeList.put(rune.id.toString(), rune);
            } catch (Exception e) {
                throw new RuntimeException("Should'nt have happened", e) ;
            }

        }
        return runeList ;
    }

    public List<Mastery> getMasteries() {
        List<Mastery> masteryList = new LinkedList<>();
        String endpoint = String.format("%s/v1.2/mastery?masteryListData=all", staticEndpoint) ;
        JsonNode root = callApi(endpoint) ;
        Iterator<JsonNode> it = root.get("data").elements() ;
        while (it.hasNext()) {

            try {
                JsonNode node = it.next() ;
                masteryList.add(om.treeToValue(node, Mastery.class));
            } catch (Exception e) {
                throw new RuntimeException("Should'nt have happened", e) ;
            }

        }
        return masteryList;
    }



   //TODO: Make real tests
    public static void test1() throws IOException {
        RiotApi r = new RiotApi(Platform.EUW1) ;
        r.throttler = new SimpleThrottler(10, 500) ;
        Long challenger = r.getChallengersIds().get(0) ;
        System.out.println("Challenger id found: " + challenger);
        Summoner s = r.getSummoner(challenger) ;
        System.out.println("Account for summoner " + challenger + " is " + s.accountId);
        List<MatchReference> matches = r.getMatchList(s.accountId, 11);
        r.getMatch(matches.get(0).gameId) ;
    }

    public static void test2() throws IOException {
        RiotApi r = new RiotApi(Platform.EUW1) ;
        r.throttler = new SimpleThrottler(10, 500) ;
        Long challenger = r.getChallengersIds().get(0) ;
        System.out.println("Challenger id found: " + challenger);
        Summoner s = r.getSummoner(challenger) ;
        System.out.println("Account for summoner " + challenger + " is " + s.accountId);
        List<MatchReference> matches = r.getMatchList(s.accountId, 11);

        int i = 1;
    }

    public static void test3() throws JsonProcessingException {
        RiotApi r = new RiotApi(Platform.EUW1) ;
        r.throttler = new SimpleThrottler(10, 500) ;
        r.getLeaguePosition(81198228L) ;
    }

    public static void main(String[] args) throws IOException {
        //new RiotApi(Platform.EUW).getChampionsList() ;
        //new RiotApi(Platform.KR).getMatchList(2035958L, 1) ;
//        for (String s : new RiotApi(Platform.EUW).getMastersIds()) {
//            System.out.println(s);
//        }
//        for (String s : new RiotApi(Platform.EUW).getChallengersIds()) {
//            System.out.println(s);
//        }

        //new RiotApi(Platform.EUW).getMatch(2969769203L, false) ;
        //new RiotApi(Platform.EUNE).getSummonerMatchDetails(1585972833L) ;
       //new RiotApi(Platform.EUW1).getChallengersIds() ;
        test3();

    }


}
