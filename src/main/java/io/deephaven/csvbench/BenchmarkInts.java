package io.deephaven.csvbench;

import gnu.trove.list.array.*;
import io.deephaven.csv.util.Renderer;
import org.assertj.core.api.Assertions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class BenchmarkInts {
    private Random rng;
    private TableTextAndData tableTextAndData;
    private ByteArrayInputStream tableTextStream;
    private int[] expectedResult;
    private int[] actualResult;

    public void setup() {
        rng = new Random(12345);
        tableTextAndData = makeTable(rng, 1000, 1);
        tableTextStream = new ByteArrayInputStream(tableTextAndData.text().getBytes(StandardCharsets.UTF_8));
        expectedResult = (int[])tableTextAndData.columns()[0];
    }

    public void checkResult() {
        Assertions.assertThat(actualResult).isEqualTo(expectedResult);
    }

    public void teardown() {
    }

    public static TableTextAndData makeTable(final Random rng, final int numRows, final int numCols) {
        final List<ColumnTextAndData<int[]>> tvs = new ArrayList<>();
        for (int ii = 0; ii < numCols; ++ii) {
            tvs.add(makeIntegerColumn(rng, numRows));
        }
        return TableTextAndData.of(tvs);
    }

    private static ColumnTextAndData<int[]> makeIntegerColumn(Random rng, final int numRows) {
        final String[] text = new String[numRows];
        final int[] data = new int[numRows];
        for (int ii = 0; ii < numRows; ++ii) {
            final int nextValue = rng.nextInt();
            data[ii] = nextValue;
            text[ii] = Integer.toString(nextValue);
        }
        return new ColumnTextAndData<>(text, data);
    }

    public void deephaven() throws io.deephaven.csv.util.CsvReaderException {
        final io.deephaven.csv.reading.CsvReader reader = new io.deephaven.csv.reading.CsvReader();
        final io.deephaven.csv.sinks.SinkFactory sf = MySinkFactory.create();
        final io.deephaven.csv.reading.CsvReader.Result result = reader.read(tableTextStream, sf);

        MySinkFactory.ResultProvider<?> rp = (MySinkFactory.ResultProvider<?>) result.columns()[0];
        actualResult = (int[]) rp.toResult();
    }

    public void apacheCommons() throws IOException {
        final org.apache.commons.csv.CSVFormat format = org.apache.commons.csv.CSVFormat.DEFAULT
                .builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setRecordSeparator('\n')
                .build();

        final org.apache.commons.csv.CSVParser parser =
                new org.apache.commons.csv.CSVParser(new StringReader(tableTextAndData.text()), format);

        final TIntArrayList results = new TIntArrayList();
        for (org.apache.commons.csv.CSVRecord next : parser) {
            results.add(Integer.parseInt(next.get(0)));
        }
        actualResult = results.toArray();
    }

    public void fastCsv() {
        final de.siegmar.fastcsv.reader.CloseableIterator<de.siegmar.fastcsv.reader.CsvRow> iterator = de.siegmar.fastcsv.reader.CsvReader.builder()
                .build(tableTextAndData.text())
                .iterator();

        final TIntArrayList results = new TIntArrayList();
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }
        while (iterator.hasNext()) {
            final de.siegmar.fastcsv.reader.CsvRow next = iterator.next();
            results.add(Integer.parseInt(next.getField(0)));
        }
        actualResult = results.toArray();
    }

    public void jacksonCsv() throws IOException {
        final com.fasterxml.jackson.databind.MappingIterator<List<String>> iterator = new com.fasterxml.jackson.dataformat.csv.CsvMapper()
                .enable(com.fasterxml.jackson.dataformat.csv.CsvParser.Feature.WRAP_AS_ARRAY)
                .readerFor(List.class)
                .readValues(tableTextAndData.text());

        final TIntArrayList results = new TIntArrayList();
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }
        while (iterator.hasNext()) {
            final List<String> next = iterator.next();
            results.add(Integer.parseInt(next.get(0)));
        }
        actualResult = results.toArray();
    }

    public void openCsv() throws IOException, com.opencsv.exceptions.CsvValidationException {
        final com.opencsv.CSVReader csvReader = new com.opencsv.CSVReader(new StringReader(tableTextAndData.text()));
        final TIntArrayList results = new TIntArrayList();
        if (csvReader.readNext() == null) {
            throw new RuntimeException("Expected header row");
        }
        while (true) {
            final String[] next = csvReader.readNext();
            if (next == null) {
                break;
            }
            results.add(Integer.parseInt(next[0]));
        }
        actualResult = results.toArray();
    }

    public void simpleFlatMapper() throws IOException {
        Iterator<String[]> iterator = org.simpleflatmapper.lightningcsv.CsvParser.iterator(tableTextAndData.text());
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }

        final TIntArrayList results = new TIntArrayList();
        while (iterator.hasNext()) {
            final String[] next = iterator.next();
            results.add(Integer.parseInt(next[0]));
        }
        actualResult = results.toArray();
    }

    public void superCsv() throws IOException {
        final org.supercsv.io.CsvListReader csvReader = new org.supercsv.io.CsvListReader(new StringReader(tableTextAndData.text()),
                org.supercsv.prefs.CsvPreference.STANDARD_PREFERENCE);
        final TIntArrayList results = new TIntArrayList();
        if (csvReader.read() == null) {
            throw new RuntimeException("Expected header row");
        }
        while (true) {
            final List<String> next = csvReader.read();
            if (next == null) {
                break;
            }
            results.add(Integer.parseInt(next.get(0)));
        }
        actualResult = results.toArray();
    }

    public void univocity() {
        final com.univocity.parsers.csv.CsvParserSettings settings = new com.univocity.parsers.csv.CsvParserSettings();
        settings.setNullValue("");
        final com.univocity.parsers.csv.CsvParser parser = new com.univocity.parsers.csv.CsvParser(settings);
        parser.beginParsing(new StringReader(tableTextAndData.text()));

        if (parser.parseNext() == null) {
            throw new RuntimeException("Expected header row");
        }
        final TIntArrayList results = new TIntArrayList();
        while (true) {
            final String[] next = parser.parseNext();
            if (next == null) {
                break;
            }
            results.add(Integer.parseInt(next[0]));
        }
        actualResult = results.toArray();
    }
}
