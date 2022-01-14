package io.deephaven;

import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.util.Renderer;

import java.io.InputStream;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

public class TestInts {

    public static void deephaven() {
        final CsvReader reader = new CsvReader();
        reader.read(rs, new MySinkFactory());
    }
}

class Freak {
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
        return TextAndValues(text, data);
    }
}



// integrals (1..N columns)
// floatings (1..N columns)
// datetimes (1..N columns)
// booleans (1..N columns)

class Junk {
    public static String makeIntegrals(final String header, final long begin, final long end) {
        final StringBuilder sb = new StringBuilder(header);
        sb.append('\n');
        for (long current = begin; current != end; ++current) {
            sb.append(current);
            sb.append('\n');
        }
        return sb.toString();
    }
}

/**
 *
 * @param <TCOL1> the
 */
interface ZamboniInterface<RESULT> {
    RESULT parse(InputStream is);
}


interface ZamboniInterface1<TCOL> {
    TARRAY
}

interface ZamboniInterface3<TCOL1, TCOL2, TCOL3> {
    Result<TCOL1, TCOL2, TCOL3> parse(InputStream is);

    class Result<TCOL1, TCOL2, TCOL3> {
        final TCOL1 col1;
        final TCOL2 col2;
        final TCOL3 col3;

    }


}