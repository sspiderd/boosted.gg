package gg.boosted.utilities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 *
 * A class with a static method to split arrays by a given chunkSize
 *
 * Created by ilan on 9/9/16.
 */
public class ArrayChunker<T> {


    public static <T extends Object> List<T[]> split(T[] array, int chunkSize){

        int x = array.length / chunkSize;
        int r = (array.length % chunkSize); // remainder

        int lower = 0;
        int upper = 0;

        List<T[]> list = new ArrayList<T[]>();

        for(int i=0; i<x; i++){
            upper += chunkSize;
            list.add(Arrays.copyOfRange(array, lower, upper));
            lower = upper;
        }
        if(r > 0){
            list.add(Arrays.copyOfRange(array, lower, (lower + r)));
        }
        return list;
    }

    public static void main(String[] args) {
        Integer[] arr = new Integer[93] ;
        Random rand = new Random() ;
        for (int i = 0; i < arr.length; i++) {
            arr[i] = rand.nextInt() ;
        }
        List<Integer[]> chunked = split(arr, 40) ;
        int total = 0 ;
        for (Integer[] chunk : chunked) {
            total += chunk.length ;
        }
        System.out.println(total);

    }
}