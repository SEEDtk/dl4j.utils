/**
 *
 */
package org.theseed.dl4j.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.theseed.io.TabbedLineReader;

/**
 * @author Bruce Parrello
 *
 */
public class TestConversion {

    @Test
    public void testSmallConversion() throws IOException {
        File inFile = new File("data", "small.ABDE.tbl");
        File targetFile = new File("data", "small.EABF.tbl");
        ConversionStream converted = new ConversionStream(inFile, targetFile);
        try (TabbedLineReader convertedStream = new TabbedLineReader(converted)) {
            assertThat(convertedStream.findField("sample_id"), equalTo(0));
            assertThat(convertedStream.findField("partE"), equalTo(1));
            assertThat(convertedStream.findField("partA"), equalTo(2));
            assertThat(convertedStream.findField("partB"), equalTo(3));
            assertThat(convertedStream.findField("partF"), equalTo(4));
            assertThat(convertedStream.findField("type"), equalTo(5));
            assertThat(convertedStream.size(), equalTo(6));
            assertThat(convertedStream.hasNext(), equalTo(true));
            TabbedLineReader.Line line = convertedStream.next();
            assertThat(line.get(0), equalTo("sampleN"));
            assertThat(line.get(1), equalTo("5.0"));
            assertThat(line.get(2), equalTo("1.0"));
            assertThat(line.get(3), equalTo("2.0"));
            assertThat(line.get(4), equalTo("0.0"));
            assertThat(line.get(5), equalTo("N"));
            assertThat(convertedStream.hasNext(), equalTo(true));
            line = convertedStream.next();
            assertThat(line.get(0), equalTo("sampleY"));
            assertThat(line.get(1), equalTo("5.1"));
            assertThat(line.get(2), equalTo("1.1"));
            assertThat(line.get(3), equalTo("2.1"));
            assertThat(line.get(4), equalTo("0.0"));
            assertThat(line.get(5), equalTo("Y"));
            assertThat(convertedStream.hasNext(), equalTo(false));
        }
    }


}
