package gg.boosted;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import gg.boosted.dtos.Champion;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by ilan on 12/9/16.
 */
public class RiotApi {

    private Region region ;
    private String riotApiKey ;
    private String regionApiLocation ;
    Client client = ClientBuilder.newClient() ;
    ObjectMapper om = new ObjectMapper() ;

    public RiotApi(Region region) {
        this.region = region ;
        riotApiKey = System.getenv("RIOT_API_KEY") ;
        if (riotApiKey == null) {
            throw new RuntimeException("You need to set environment variable \"RIOT_API_KEY\" with your riot api key") ;
        }
        regionApiLocation = "https://" +
                region.toString().toLowerCase() +
                ".api.pvp.net/api/lol/" +
                region.toString().toLowerCase() + "/" ;
    }

    public List<Champion> getChampionsList() throws IOException {
        String endpoint = "https://global.api.pvp.net/api/lol/static-data/" + region.toString().toLowerCase() + "/v1.2/champion?api_key=" + riotApiKey;
        WebTarget target = client.target(endpoint) ;
        String response = target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class) ;
        JsonNode rootNode = om.readValue(response, JsonNode.class);

        List<Champion> champions = new LinkedList<>() ;
        //Riot has made this api a little silly, so i need to be silly to get the data
        Iterator<Map.Entry<String, JsonNode>> it = rootNode.get("data").fields() ;
        while (it.hasNext()) {
            JsonNode node = it.next().getValue() ;
            champions.add(om.treeToValue(node, Champion.class)) ;
        }
        return champions ;
    }

    public static void main(String[] args) throws IOException {
        new RiotApi(Region.EUW).getChampionsList() ;
    }


}
