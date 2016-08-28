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

    public static Role byId(Integer roleId) {
        for (Role role : Role.values()) {
            if (role.roleId == roleId) {
                return role ;
            }
        }
        throw new RuntimeException("Unknown roleId " + roleId) ;
    }
}
