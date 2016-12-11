package gg.boosted;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import gg.boosted.constants.QueueType;
import gg.boosted.dtos.Champion;
import gg.boosted.dtos.MatchReference;
import gg.boosted.throttlers.IThrottler;
import gg.boosted.throttlers.SimpleThrottler;
import gg.boosted.utilities.ArrayChunker;
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
    Client client = ClientBuilder.newClient() ;
    ObjectMapper om = new ObjectMapper() ;
    private IThrottler throttler = new SimpleThrottler(10, 500) ;

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
    }

    private JsonNode callApi(String endpoint) {
        WebTarget target = client.target(endpoint) ;
        //Don't stop believing
        while (true) {
            try {
                throttler.waitFor();
                log.debug("API Called: {}", endpoint);
                String response = target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
                return om.readValue(response, JsonNode.class);
            } catch (ClientErrorException cer) {
                int status = cer.getResponse().getStatus() ;
                String error = String.format("Bad status: {%d} -> {%s}", status, cer.getResponse().getStatusInfo().getReasonPhrase());
                if (status == 429) {
                    error = error.concat(" : rateLimitCount {" + cer.getResponse().getHeaderString("X-Rate-Limit-Count") + "}") ;
                    String retryAfter = cer.getResponse().getHeaderString("Retry-After") ;
                    if (retryAfter != null) {
                        error = error.concat(" : retryAfter {" + retryAfter + "}") ;
                    }
                }
                log.error(error);
            } catch (Exception ex) {
                log.error("Logged unknown error", ex) ;
                throw new RuntimeException(ex) ;
            }
        }
    }

    public List<Champion> getChampionsList() throws IOException {
        String endpoint = "https://global.api.pvp.net/api/lol/static-data/" + region.toString().toLowerCase() + "/v1.2/champion?api_key=" + riotApiKey;

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
                "&beginTime=" + beginTime +
                "&api_key=" + riotApiKey ;

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
                ".api.pvp.net/observer-mode/rest/featured" +
                "?api_key=" + riotApiKey ;
        JsonNode rootNode = callApi(endpoint) ;
        return rootNode.toString() ;
    }

    public Map<String, String> getSummonerIdsByNames(String... names) throws IOException {
        Map<String, String> map = new HashMap<>() ;
        //We can get 40 names for each call, so
        List<String[]> chunckedArray = ArrayChunker.split(names, 40) ;

        for (String[] array : chunckedArray) {
            String endpoint = regionEndpoint +
                    "/v1.4/summoner/by-name/" ;
            endpoint += Arrays.stream(array).collect(Collectors.joining(",")) ;
            endpoint += "&api_key=" + riotApiKey ;
            JsonNode rootNode = callApi(endpoint) ;
            //Riot has made this api a little silly, so i need to be silly to get the data
            Iterator<Map.Entry<String, JsonNode>> it = rootNode.fields() ;
            while (it.hasNext()) {
                JsonNode node = it.next().getValue() ;
                map.put(node.get("name").asText(), node.get("id").asText()) ;
            }
        }
        return map ;
    }

    public List<String> getChallengersIds() {
        List<String> challengerIds = new LinkedList<>() ;
        String endpoint = String.format("%s/v2.5/league/challenger?type=%s&api_key=%s", regionEndpoint, QueueType.RANKED_SOLO_5x5, riotApiKey) ;
        JsonNode root = callApi(endpoint) ;
        Iterator<JsonNode> it = root.get("entries").elements() ;
        while (it.hasNext()) {
            JsonNode node = it.next() ;
            challengerIds.add(node.get("playerOrTeamId").asText());
        }
        return challengerIds ;
    }

    public List<String> getMastersIds() {
        List<String> challengerIds = new LinkedList<>() ;
        String endpoint = String.format("%s/v2.5/league/master?type=%s&api_key=%s", regionEndpoint, QueueType.RANKED_SOLO_5x5, riotApiKey) ;
        JsonNode root = callApi(endpoint) ;
        Iterator<JsonNode> it = root.get("entries").elements() ;
        while (it.hasNext()) {
            JsonNode node = it.next() ;
            challengerIds.add(node.get("playerOrTeamId").toString());
        }
        return challengerIds ;
    }


    public static void main(String[] args) throws IOException {
        //new RiotApi(Region.EUW).getChampionsList() ;
        //new RiotApi(Region.KR).getMatchList(2035958L, 1) ;
        for (String s : new RiotApi(Region.EUW).getMastersIds()) {
            System.out.println(s);
        }
        for (String s : new RiotApi(Region.EUW).getChallengersIds()) {
            System.out.println(s);
        }
    }


}
