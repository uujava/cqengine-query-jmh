package ru.programpark.tests.perf.query;

import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.resultset.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by kozyr on 14.11.2016.
 */
public class Sequence {
    private static final Random r = new Random(System.currentTimeMillis());
    volatile static private long counter = 0;

    public static long next() {
        return counter += 1;
    }

    public static int random(int length) {
        return r.nextInt(length);
    }

    public static List<Long> randomList(Long[] ids, int size) {
        ArrayList<Long> longs = new ArrayList<Long>(size);
        for (int i = 0; i < size; i++) {
            longs.add(randomObject(ids));
        }
        return longs;
    }

    public static <T> T randomObject(T[] ids) {
        int random = random(ids.length);
        return ids[random];
    }

    public static <T> Stream<T> asStream(ResultSet<T> rs) {
        return StreamSupport.stream(rs.spliterator(), false);
    }

    public static <T> Stream<T> asStream(IndexedCollection<T> second, Query<T> equal, QueryOptions options) {
        return asStream(second.retrieve(equal, options));
    }
}
