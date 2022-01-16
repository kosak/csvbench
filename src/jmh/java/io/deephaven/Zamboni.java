package io.deephaven;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class Zamboni {
    int x = 923;
    int y = 123;

    @Benchmark
    @Warmup(iterations = 10, time = 3, timeUnit = TimeUnit.SECONDS)
    public int baseline() {
        return x;
    }

    @Benchmark
    @Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
    public int sum() {
        return x + y;
    }
}
