package gg.boosted;

/**
 * Created by ilan on 8/22/16.
 */
public enum Tier {
    BRONZE(0),
    SILVER(500),
    GOLD(1000),
    PLATINUM(1500),
    DIAMOND(2000),
    MASTER(2500),
    CHALLENGER(3000) ;

    public int tierId;

    Tier(int tierId) {
        this.tierId = tierId;
    }

    Tier byId(int tierId) {
        for (Tier tier : Tier.values()) {
            if (tier.tierId == tierId) {
                return tier ;
            }
        }
        throw new RuntimeException("Unknown tierId " + tierId) ;
    }
}
