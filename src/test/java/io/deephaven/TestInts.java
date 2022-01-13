package io.deephaven;

import io.deephaven.csv.reading.CsvReader;

public class TestInts {
    public static void startup() {
        final String header = "Values\n";
        final StringBuilder sb = new StringBuilder();
        for (int ii = 0; ii < 10_000; ++ii) {
            sb.append(ii);
            sb.append('\n');
        }
        // 10,000 x 1000 = 10 million rows
        final RepeatedStream rs = new RepeatedStream(header, sb.toString(), 1000);
    }

    public static void deephaven() {
        final CsvReader reader = new CsvReader();
        reader.read(rs, new MySinkFactory());
    }
}
