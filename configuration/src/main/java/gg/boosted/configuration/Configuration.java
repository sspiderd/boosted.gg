package gg.boosted.configuration;

import java.io.InputStream;
import java.util.Properties;

/**
 *
 * Basically a delegator class for configurations
 *
 * Created by ilan on 12/14/16.
 */
public class Configuration {

    private static Properties props = new Properties() ;

    static {
        try (InputStream stream =
                     Configuration.class.getResourceAsStream("/configuration.properties")) {
            props.load(stream);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to load configuration file") ;
        }
    }

    public static Object get(String property) {
        Object o = props.get(property);
        if (o == null) {
            throw new RuntimeException("Could not find property '" + property + "' in configuration") ;
        }
        return o ;
    }

    public static String getString(String property) {
        return get(property).toString() ;
    }

    public static Integer getInt(String property) {
        return Integer.parseInt(getString(property)) ;
    }

    public static Long getLong(String property) {
        return Long.parseLong(getString(property)) ;
    }


    public static void main(String[] args) {
        System.out.println(Configuration.getString("cassandra.location"));
    }

}
