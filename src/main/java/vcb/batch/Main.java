package vcb.batch;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

	private static final String PDF_FILE_PATH = "C:\\Workspace\\FPT-IS\\Transform-Account-Statement-VCB\\Account-Statement-VCB\\src\\main\\resources\\input\\10092024.pdf";
	private static final Pattern DATE_PATTERN = Pattern.compile("\\r?\\n(\\d{2}/\\d{2}/\\d{4})\\r?\\n");
	private static final Pattern ROW_PATTERN = Pattern.compile("(\\d{2}/\\d{2}/\\d{4})\\s+(\\S+)\\s+(\\S+)\\s+(.*)");

	public static void main(String[] args) throws IOException {
		Logger.getLogger("org").setLevel(Level.WARN);

		// Add hadoop env
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkSession spark = SparkSession.builder()
						.master("local[*]")
						.appName("Local iceberg test")
				        .getOrCreate();

		String pdfContent = extractPdfContent(PDF_FILE_PATH);
		String cleanedContent = cleanPdfContent(pdfContent);
		List<String> segments = splitContentIntoSegments(cleanedContent);
		Dataset<Row> df = createDataFrame(spark, segments);
		df.show();

		spark.close();
	}

	private static String extractPdfContent(String pdfFilePath) throws IOException {
		try (PDDocument document = PDDocument.load(new File(pdfFilePath))) {
			PDFTextStripper pdfStripper = new PDFTextStripper();
			pdfStripper.setStartPage(1);
			pdfStripper.setEndPage(document.getNumberOfPages());
			return pdfStripper.getText(document);
		}
	}

	private static String cleanPdfContent(String content) {
		String[] unwantedStrings = {
				"Telex\\s+:\\s+\\(0805\\)\\s+411504\\s+VCB\\s+-\\s+VT\\s+Swift\\s+:\\s+BFTV\\s+VNVX",
				"Website:\\s+www.vietcombank.com.vn",
				"Contact center:\\s+1900.54.54.13",
				"Postal\\s+address:\\s+198\\s+TRAN\\s+QUANG\\s+KHAI\\s+AVENUE\\s+HANOI\\s+-\\s+S\\.R\\.\\s+VIETNAM",
				"Page \\d+ of \\d+",
				"Số\\s+CT/\\s+Doc\\s+No",
				"Số\\s+tiền\\s+ghi\\s+nợ/\\s+Debit",
				"Số\\s+tiền\\s+ghi\\s+có/\\s+Credit",
				"Số\\s+dư/\\s+Balance",
				"Nội\\s+dung\\s+chi\\s+tiết/\\s+Transactions\\s+in\\s+detail"
		};

		for (String s : unwantedStrings) {
			content = content.replaceAll(s, "");
		}
		return content;
	}

	private static List<String> splitContentIntoSegments(String content) {
		Matcher matcher = DATE_PATTERN.matcher(content);
		List<String> segments = new java.util.ArrayList<>();
		int start = 0;

		while (matcher.find()) {
			int end = matcher.start();
			if (start < end) {
				segments.add(content.substring(start, end).trim().replaceAll("\\r?\\n+", " "));
			}
			start = matcher.start();
		}

		if (start < content.length()) {
			segments.add(content.substring(start).trim().replaceAll("\\r?\\n+", " "));
		}

		segments.remove(0); // Remove header
		return segments;
	}

	private static Dataset<Row> createDataFrame(SparkSession spark, List<String> segments) {
		List<Row> rows = new ArrayList<>();
		for (String segment : segments) {
			Row row = parseRow(segment);
			if (row.get(0) != null) {
				rows.add(row);
			}
		}

		StructType schema = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("date", DataTypes.StringType, false),
				DataTypes.createStructField("transaction_code", DataTypes.StringType, false),
				DataTypes.createStructField("amount", DataTypes.StringType, false),
				DataTypes.createStructField("content", DataTypes.StringType, false)
		));

		return spark.createDataFrame(rows, schema);
	}

	private static Row parseRow(String row) {
		Matcher matcher = ROW_PATTERN.matcher(row);
		if (matcher.matches()) {
			return RowFactory.create(
					matcher.group(1).trim(),
					matcher.group(2).trim(),
					matcher.group(3).trim(),
					matcher.group(4).trim()
			);
		}
		return RowFactory.create(null, null, null, row);
	}
}