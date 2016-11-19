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

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static ru.programpark.tests.perf.query.Fields.generate;
import static ru.programpark.tests.perf.query.Sequence.asStream;
import static ru.programpark.tests.perf.query.Sequence.randomObject;

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

    // generate random or sequential data
    @Param({"true"})
    boolean random;
    // use cached options
    @Param({"true"})
    boolean cachedOptions;

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
    private QueryOptions options;

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        first = new ConcurrentIndexedCollection<>();
        second = new ConcurrentIndexedCollection<>();
        options = cachedOptions ? new QueryOptions() : null;

        objIds = random ? generate(MAX_IDS, MAX_IDS * 1000) : generate(MAX_IDS);
        // fill both collections with same values
        Stream.of(objIds).distinct()
                .forEach(v -> {
                    first.add(v);
                    second.add(v);
                });

        first.addIndex(UniqueIndex.onAttribute(ID));

        // we do not query by PK on second collection - do not index it
        // as if we had FK in different collection
        second.addIndex(HashIndex.onAttribute(EID));
    }


    @Benchmark
    public void existsJoin(Blackhole bh) {
        Query<Long> query = QueryFactory.and(QueryFactory.equal(ID, randomObject(objIds)), QueryFactory.existsIn(second, ID, EID));
        bh.consume(first.retrieve(query, getOptions()).iterator().next());
    }

    @Benchmark
    public void existsJoinWithSubquery(Blackhole bh) {
        Long pkValue = randomObject(objIds);
        Equal<Long, Long> firstEqual = QueryFactory.equal(ID, pkValue);
        Equal<Long, Long> existEqual = QueryFactory.equal(EID, pkValue);
        Query<Long> query = QueryFactory.and(firstEqual, QueryFactory.existsIn(second, ID, EID, existEqual));
        bh.consume(first.retrieve(query, getOptions()).iterator().next());
    }

    @Benchmark
    public void iteratorJoinByEID(Blackhole bh) {
        Equal<Long, Long> equal = QueryFactory.equal(EID, randomObject(objIds));
        Long found = second.retrieve(equal, getOptions()).iterator().next();
        Equal<Long, Long> subQ = QueryFactory.equal(ID, found);
        bh.consume(first.retrieve(subQ, getOptions()).iterator().next());
    }

    @Benchmark
    public void streamJoinByEID(Blackhole bh) {
        Long value = randomObject(objIds);
        Equal<Long, Long> equal = QueryFactory.equal(EID, value);
        Long result = asStream(second, equal, getOptions()).map(found -> {
            Equal<Long, Long> subQ = QueryFactory.equal(ID, found);
            return first.retrieve(subQ, getOptions()).iterator().next();
        }).findFirst().get();
        bh.consume(result);
    }

    @Benchmark
    public void iteratorJoinByEIDWithClose(Blackhole bh) {
        Equal<Long, Long> equal = QueryFactory.equal(EID, randomObject(objIds));
        ResultSet retrieve = second.retrieve(equal, getOptions());
        Long found = (Long) retrieve.iterator().next();
        retrieve.close();
        Equal<Long, Long> subQ = QueryFactory.equal(ID, found);
        ResultSet result = first.retrieve(subQ, getOptions());
        bh.consume(result.iterator().next());
        result.close();
    }

    @Benchmark
    public void joinByEID(Blackhole bh) {
        Query<Long> query = QueryFactory.and(QueryFactory.existsIn(second, ID, EID), QueryFactory.equal(EID, randomObject(objIds)));
        bh.consume(first.retrieve(query, getOptions()).iterator().next());
    }

    @Benchmark
    public void cubeExistsJoinFirstEntry(Blackhole bh) {
        bh.consume(first.retrieve(QueryFactory.existsIn(second, ID, EID), getOptions()).iterator().next());
    }

    @Benchmark
    public void uniqueQuery(Blackhole bh) {
        Query<Long> query = QueryFactory.equal(ID, randomObject(objIds));
        bh.consume(first.retrieve(query, getOptions()).iterator().next());
    }


    private QueryOptions getOptions() {
        return options == null ? new QueryOptions() : options;
    }
}
