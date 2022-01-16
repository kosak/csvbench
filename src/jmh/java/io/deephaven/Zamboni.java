package io.deephaven;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 3,
        jvmArgsAppend = {"-server", "-disablesystemassertions"})
public class MyBenchmark {

    int x = 923;
    int y = 123;

    @GenerateMicroBenchmark
    @Warmup(iterations = 10, time = 3, timeUnit = TimeUnit.SECONDS)
    public int baseline() {
        return x;
    }

    @GenerateMicroBenchmark
    @Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
    public int sum() {
        return x + y;
    }
}
