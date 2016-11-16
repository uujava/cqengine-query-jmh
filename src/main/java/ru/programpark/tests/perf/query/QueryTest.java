package ru.programpark.tests.perf.query;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.index.hash.HashIndex;
import com.googlecode.cqengine.index.unique.UniqueIndex;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.QueryFactory;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.query.simple.Equal;
import com.googlecode.cqengine.resultset.ResultSet;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static ru.programpark.tests.perf.query.Fields.generate;

/**
 * Tests on a Long collection
 */
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class QueryTest {

    public static final int MAX_IDS = 1000000;
    // stores all keys here
    private Long[] objIds;
    // indexed collection of Long under test
    private IndexedCollection<Long> first;
    private IndexedCollection<Long> second;
    // keys hash to compare cqengine  results with hash
    private Map<Long, Long> firstHash = new ConcurrentHashMap<>();
    private Map<Long, List<Long>> fkHash = new ConcurrentHashMap<>();

    // generate random or sequential data
    @Param({"true", "false"})
    boolean random;

    private static final SimpleAttribute<Long, Long> ID = new SimpleAttribute<Long, Long>(Long.class, Long.class, "id") {
        @Override
        public Long getValue(Long o, QueryOptions queryOptions) {
            return o;
        }
    };
    // same as ID, but different name to be closer to real case
    private static final SimpleAttribute<Long, Long> EID = new SimpleAttribute<Long, Long>(Long.class, Long.class, "eid") {
        @Override
        public Long getValue(Long o, QueryOptions queryOptions) {
            return o;
        }
    };

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        first = new ConcurrentIndexedCollection<>();
        second = new ConcurrentIndexedCollection<>();
        // keys hash to compare cqengine results with hash
        firstHash = new ConcurrentHashMap<>(MAX_IDS);
        fkHash = new ConcurrentHashMap<>(MAX_IDS);
        objIds = random ? generate(MAX_IDS, MAX_IDS * 1000) : generate(MAX_IDS);
        // fill both collections and a map with same values
        Stream.of(objIds).distinct()
                .forEach(v -> {
                    first.add(v);
                    second.add(v);
                    firstHash.put(v, v);
                    fkHash.put(v, Arrays.asList(v));
                });

        first.addIndex(UniqueIndex.onAttribute(ID));

        // we do not query by PK on second collection - do not index it
        // as if we had FK in different collection
        second.addIndex(HashIndex.onAttribute(EID));
    }

    @Benchmark
    public void randomBaseline(Blackhole bh) {
        bh.consume(randomId());
    }

    @Benchmark
    public void createIdQuery(Blackhole bh) {
        Equal equal = QueryFactory.equal(ID, randomId());
        bh.consume(equal);
    }

    private Long randomId() {
        return objIds[Sequence.random(MAX_IDS)];
    }

    @Benchmark
    public void createExistsQuery(Blackhole bh) {
        Query equal = QueryFactory.existsIn(second, ID, EID);
        bh.consume(equal);
    }

    @Benchmark
    public void createUniqueExistsQuery(Blackhole bh) {
        Query query = QueryFactory.and(QueryFactory.equal(ID, randomId()), QueryFactory.existsIn(second, ID, EID));
        bh.consume(query);
    }

    @Benchmark
    public void uniqueExistsJoin(Blackhole bh) {
        Query<Long> query = QueryFactory.and(QueryFactory.equal(ID, randomId()), QueryFactory.existsIn(second, ID, EID));
        bh.consume(first.retrieve(query).iterator().next());
    }

    @Benchmark
    public void uniqueUniqueExistsSelfJoin(Blackhole bh) {
        Long pkValue = randomId();
        Equal<Long, Long> firstEqual = QueryFactory.equal(ID, pkValue);
        Equal<Long, Long> existEqual = QueryFactory.equal(EID, pkValue);
        Query<Long> query = QueryFactory.and(firstEqual, QueryFactory.existsIn(second, ID, EID, existEqual));
        bh.consume(first.retrieve(query).iterator().next());
    }

    @Benchmark
    public void uniqueJoin(Blackhole bh) {
        Equal<Long, Long> equal = QueryFactory.equal(EID, randomId());
        Long found = second.retrieve(equal).iterator().next();
        Equal<Long, Long> subQ = QueryFactory.equal(ID, found);
        bh.consume(first.retrieve(subQ).iterator().next());
    }

    @Benchmark
    public void streamJoin(Blackhole bh) {
        Long value = randomId();
        Equal<Long, Long> equal = QueryFactory.equal(EID, value);
        Long result = StreamSupport.stream(second.retrieve(equal).spliterator(), false).map(found -> {
            Equal<Long, Long> subQ = QueryFactory.equal(ID, found);
            return first.retrieve(subQ).iterator().next();
        }).findFirst().get();
        bh.consume(result);
    }

    @Benchmark
    public void uniqueJoinWithClose(Blackhole bh) {
        Equal<Long, Long> equal = QueryFactory.equal(EID, randomId());
        ResultSet retrieve = second.retrieve(equal);
        Long found = (Long) retrieve.iterator().next();
        retrieve.close();
        Equal<Long, Long> subQ = QueryFactory.equal(ID, found);
        ResultSet result = first.retrieve(subQ);
        bh.consume(result.iterator().next());
        result.close();
    }

    @Benchmark
    public void uniqueReversedSelfJoin(Blackhole bh) {
        Query<Long> query = QueryFactory.and(QueryFactory.existsIn(second, ID, EID), QueryFactory.equal(EID, randomId()));
        bh.consume(first.retrieve(query).iterator().next());
    }

    @Benchmark
    public void selfJoinFirstEntry(Blackhole bh) {
        bh.consume(first.retrieve(QueryFactory.existsIn(second, ID, EID)).iterator().next());
    }

    @Benchmark
    public void fullSelfJoin(Blackhole bh) {
        ResultSet<Long> resultSet = first.retrieve(QueryFactory.existsIn(second, ID, EID));
        for (Long aResultSet : resultSet) {
            bh.consume(aResultSet);
        }
    }

    @Benchmark
    public void queryHash(Blackhole bh) {
        bh.consume(firstHash.get(randomId()));
    }

    @Benchmark
    public void joinHash(Blackhole bh) {
        Long key = randomId();
        Iterator<Long> iterator = fkHash.get(key).iterator();
        while (iterator.hasNext()) {
            Long next = iterator.next();
            bh.consume(firstHash.get(next));
        }
    }

    @Benchmark
    public void uniqueQuery(Blackhole bh) {
        Query<Long> query = QueryFactory.equal(ID, randomId());
        bh.consume(first.retrieve(query).iterator().next());
    }
}
