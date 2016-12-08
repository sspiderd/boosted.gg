package gg.boosted;

/**
 * Created by ilan on 8/16/16.
 */
public enum Role {

    TOP(1),
    MIDDLE(2),
    JUNGLE(3),
    BOTTOM(4),
    SUPPORT(5) ;

    public int roleId ;

    Role(int roleId) {
        this.roleId = roleId ;
    }

}
