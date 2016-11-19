package ru.programpark.tests.perf.query;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static ru.programpark.tests.perf.query.Fields.generate;
import static ru.programpark.tests.perf.query.Sequence.randomObject;

/**
 * Tests on a Long collection
 */
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class HashTest {

    public static final int MAX_IDS = 1000000;
    // stores all keys here
    private Long[] objIds;
    // keys hash to compare cqengine  results with hash
    private Map<Long, Long> firstHash = new ConcurrentHashMap<>();
    private Map<Long, List<Long>> fkHash = new ConcurrentHashMap<>();

    // generate random or sequential data
    @Param({"true", "false"})
    boolean random;
    // generate random or sequential data

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        // keys hash to compare cqengine results with hash
        firstHash = new ConcurrentHashMap<>(MAX_IDS);
        fkHash = new ConcurrentHashMap<>(MAX_IDS);
        objIds = random ? generate(MAX_IDS, MAX_IDS * 1000) : generate(MAX_IDS);
        // fill both collections and a map with same values
        Stream.of(objIds).distinct()
                .forEach(v -> {
                    firstHash.put(v, v);
                    fkHash.put(v, Arrays.asList(v));
                });
    }

    @Benchmark
    public void randomBaseline(Blackhole bh) {
        bh.consume(randomObject(objIds));
    }

    @Benchmark
    public void queryHash(Blackhole bh) {
        bh.consume(firstHash.get(randomObject(objIds)));
    }

    @Benchmark
    public void joinHash(Blackhole bh) {
        Long key = randomObject(objIds);
        Iterator<Long> iterator = fkHash.get(key).iterator();
        while (iterator.hasNext()) {
            Long next = iterator.next();
            bh.consume(firstHash.get(next));
        }
    }

}
