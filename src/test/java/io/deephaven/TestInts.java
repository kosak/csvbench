package io.deephaven;

import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.sinks.Source;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.Renderer;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.stream.IntStream;

public class TestInts {
    @Test
    public void deephaven() throws CsvReaderException {
        final Random rng = new Random(12345);
        final TextAndNubbins tns = buildTable(rng, 1000, 1);
        final InputStream bais = new ByteArrayInputStream(tns.text.getBytes(StandardCharsets.UTF_8));

        final CsvReader reader = new CsvReader();
        final SinkFactory sf = SinkFactory.of(
                null, null,
                null, null,
                MyIntSink::new, null,
                null, null,
                null, null,
                null, null,
                null, null,
                null, null,
                null, null,
                null, null,
                null);
        reader.read(bais, sf);
    }

    public static TextAndNubbins buildTable(final Random rng, final int numRows, final int numCols) {
        final TextAndValues[] tvs = new TextAndValues[numCols];
        for (int ii = 0; ii < numCols; ++ii) {
            tvs[ii] = makeIntegers(rng, numRows);
        }

        final StringBuilder sb = new StringBuilder();
        // Write a line of headers like Column1,Column2,...,ColumnN
        Renderer.renderList(sb, IntStream.range(0, numCols)::iterator, ",", i -> "Column" + (i + 1));
        sb.append('\n');
        for (int jj = 0; jj < numRows; ++jj) {
            final int finalJJ = jj;
            // Write a line of data like 12,-54321,...,17
            Renderer.renderList(sb, IntStream.range(0, numCols)::iterator, ",", i -> tvs[i].text[finalJJ]);
            sb.append('\n');
        }

        final String text = sb.toString();
        final Object[] nubbins = new Object[numCols];
        for (int ii = 0; ii < numCols; ++ii) {
            nubbins[ii] = tvs[ii].data;
        }

        return new TextAndNubbins(text, nubbins);
    }

    private static TextAndValues makeIntegers(Random rng, final int numRows) {
        final String[] text = new String[numRows];
        final int[] data = new int[numRows];
        for (int ii = 0; ii < numRows; ++ii) {
            final int nextValue = rng.nextInt();
            data[ii] = nextValue;
            text[ii] = Integer.toString(nextValue);
        }
        return new TextAndValues(text, data);
    }

    private static class TextAndNubbins {
        private final String text;
        private final Object[] nubbins;

        public TextAndNubbins(String text, Object[] nubbins) {
            this.text = text;
            this.nubbins = nubbins;
        }
    }

    private static class TextAndValues {
        private final String[] text;
        private final int[] data;

        public TextAndValues(String[] text, int[] data) {
            this.text = text;
            this.data = data;
        }
    }

    private static class MyIntSink implements Sink<int[]>, Source<int[]> {
        @Override
        public void write(int[] src, boolean[] isNull, long destBegin, long destEnd, boolean appending) {
            System.out.println("haha");
        }

        @Override
        public void read(int[] dest, boolean[] isNull, long srcBegin, long srcEnd) {
            System.out.println("haha");
        }
    }
}
