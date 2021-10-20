/**
 * 
 */
package dl4j.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.theseed.io.TabbedLineReader;

/**
 * This class takes as input two tab-delimited files, a source file and a target file.  The source file is converted
 * into the target file's columnar format.  Columns present in the source but not the target are eliminated.  Columns
 * present in the target but not the source are arbitrarily assigned a value of 0.0.
 * 
 * The process is managed by an instruction array.  For each outgoing column index, it contains the column index of the
 * input column or a -1, indicating that the column should get a default value.
 *  
 * @author Bruce Parrello
 *
 */
public class ConversionStream extends InputStream implements AutoCloseable {
	
	// FIELDS
	/** source input stream */
	private TabbedLineReader inStream;
	/** output instruction array */
	private int[] instructions;
	/** current output line */
	private String currentLine;
	/** position in current output line */
	private int currentIdx;
	/** default value to put in missing columns */
	private String defaultValue;
	/** standard default value */
	private static final String DEFAULT_VALUE = "0.0";
	
	/**
	 * Construct a conversion stream from the specified source and target files.
	 * 
	 * @param source	source file name
	 * @param target	target file name
	 * 
	 * @throws IOException 
	 */
	public ConversionStream(File source, File target) throws IOException {
		this.init(source, target, DEFAULT_VALUE);
	}
	
	/**
	 * Construct a conversion stream from the specified source and target files with the specified default column value.
	 * 
	 * @param source	source file name
	 * @param target	target file name
	 * @param defaultV	default value for missing columns
	 * 
	 * @throws IOException 
	 */
	public ConversionStream(File source, File target, String defaultV) throws IOException {
		this.init(source, target, defaultV);
	}
	
	/**
	 * Initialize a conversion stream from the specified source and target files with the specified default
	 * column value.
	 * 
	 * @param source	source file name
	 * @param target	target file name
	 * @param defaultV	default value for missing columns
	 * 
	 * @throws IOException 
	 */
	private void init(File source, File target, String defaultV) throws IOException {
		this.defaultValue = defaultV;
		// Open the source stream.  This will stay open, as we 
		this.inStream = new TabbedLineReader(source);
		// Open the target stream.  We use this to build the instruction array.
		try (TabbedLineReader targetStream = new TabbedLineReader(target)) {
			// Save the header line as the first output line and position our output process at the front.
			this.currentLine = targetStream.header();
			this.currentIdx = 0;
			// Create the instruction array.  For each output column, we find the column index of the corresponding
			// input file column.  If it is not found, we get back -1, which is what we want.
			String[] targetHeaders = targetStream.getLabels();
			this.instructions = Arrays.stream(targetHeaders).mapToInt(x -> this.inStream.findColumn(x)).toArray();
		}
	}

	@Override
	public int read() throws IOException {
		int retVal;
		// Insure we have data in the current line.
		if (this.currentLine == null) {
			// Here we have EOF.
			retVal = -1;
		} else if (this.currentIdx == this.currentLine.length()) {
			// End of the current line.  Emit an EOL and prepare the next line.
			retVal = '\n';
			this.readNextLine();
		} else {
			// Here we can get the next character.
			retVal = this.currentLine.charAt(this.currentIdx);
			this.currentIdx++;
		}
		return retVal;
	}
	
	/**
	 * Read in the next input line and convert it to an output string.
	 */
	private void readNextLine() {
		if (! this.inStream.hasNext()) {
			// Denote we're out of input data.
			this.currentLine = null;
		} else {
			// Here there is another line to read.
			TabbedLineReader.Line line = this.inStream.next();
			// Reformat this line using the instruction array.
			this.currentLine = Arrays.stream(this.instructions).mapToObj(i -> this.getField(line, i))
					.collect(Collectors.joining("\t"));
			this.currentIdx = 0;
		}
	}
	
	/**
	 * @return the input line field with the given instructional index
	 * 
	 * @param line		input line
	 * @param idx		field index, or -1 to use the default
	 */
	private String getField(TabbedLineReader.Line line, int idx) {
		String retVal;
		if (idx < 0)
			retVal = this.defaultValue;
		else
			retVal = line.get(idx);
		return retVal;
	}

	@Override
	public void close() {
		if (inStream != null) {
			inStream.close();
			inStream = null;
		}
	}
		

}
