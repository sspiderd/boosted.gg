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

    public static String getString(String property) {
        return props.getProperty(property) ;
    }

    public static Integer getInt(String property) {
        return Integer.parseInt(props.getProperty(property)) ;
    }

    public static Long getLong(String property) {
        return Long.parseLong(props.getProperty(property)) ;
    }


    public static void main(String[] args) {
        System.out.println(Configuration.getString("cassandra.location"));
    }

}
