package ru.programpark.tests.perf.query;

import java.util.Random;

/**
 * Created by kozyr on 14.11.2016.
 */
public class Sequence {
    private static final Random r = new Random(System.currentTimeMillis());
    volatile static private long counter = 0;

    public static long next(){
        return counter +=1;
    }

    public static int random(int length) {
        return r.nextInt(length);
    }
}
