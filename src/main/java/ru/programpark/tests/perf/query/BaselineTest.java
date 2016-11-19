package ru.programpark.tests.perf.query;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.QueryFactory;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.query.simple.Equal;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static ru.programpark.tests.perf.query.Sequence.*;
import static ru.programpark.tests.perf.query.Fields.generate;

/**
 * Tests on a Long collection
 */
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class BaselineTest {

    public static final int MAX_IDS = 100000;
    // stores all keys here
    private Long[] objIds;
    // indexed collection of Long under test
    private MyCollection<Long> first;

    // generate random or sequential data
    @Param({"true"})
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
        first = new MyCollection<>();
        objIds = random ? generate(MAX_IDS, MAX_IDS * 1000) : generate(MAX_IDS);
        Stream.of(objIds).distinct()
                .forEach(v -> {
                    first.add(v);
                });
    }

    @Benchmark
    public void randomBaseline(Blackhole bh) {
        bh.consume(randomObject(objIds));
    }

    @Benchmark
    public void createIdQuery(Blackhole bh) {
        Equal equal = QueryFactory.equal(ID, randomObject(objIds));
        bh.consume(equal);
    }

    @Benchmark
    public void createExistsQuery(Blackhole bh) {
        Query equal = QueryFactory.existsIn(first, ID, EID);
        bh.consume(equal);
    }

    @Benchmark
    public void createUniqueExistsQuery(Blackhole bh) {
        Query query = QueryFactory.and(QueryFactory.equal(ID, randomObject(objIds)), QueryFactory.existsIn(first, ID, EID));
        bh.consume(query);
    }

    @Benchmark
    public void createQueryOptions(Blackhole bh) {
        QueryOptions queryOptions = first.createOptions();
        bh.consume(queryOptions);
    }

    class MyCollection<T> extends ConcurrentIndexedCollection<T> {
        public QueryOptions createOptions() {
            final QueryOptions queryOptions = openRequestScopeResourcesIfNecessary(null);
            flagAsReadRequest(queryOptions);
            return queryOptions;
        }
    }


    @Benchmark
    public void randomList1Baseline(Blackhole bh) {
        bh.consume(randomList(objIds, 1));
    }

    @Benchmark
    public void randomList5Baseline(Blackhole bh) {
        bh.consume(randomList(objIds, 5));
    }

    @Benchmark
    public void randomList10Baseline(Blackhole bh) {
        bh.consume(randomList(objIds, 25));
    }

    @Benchmark
    public void randomList100Baseline(Blackhole bh) {
        bh.consume(randomList(objIds, 100));
    }

}
