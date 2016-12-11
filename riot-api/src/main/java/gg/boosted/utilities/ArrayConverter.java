package gg.boosted.utilities;

/**
 * Created by ilan on 12/12/16.
 */
public class ArrayConverter {

    public static String[] convertLongToString(Long[] from) {
        String[] to = new String[from.length] ;
        for (int i = 0; i < from.length; i++) {
            to[i] = String.valueOf(from[i]) ;
        }
        return to ;
    }
}
