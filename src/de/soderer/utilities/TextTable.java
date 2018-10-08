package de.soderer.utilities;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

public class TextTable {
	protected static final String METADATA_HEADER = "HEADER";
	protected static final String METADATA_COLOR = "COLOR";
	protected static final String METADATA_RIGHTALIGNMENT = "RIGHTALIGN";

	protected List<List<String>> columnNames;
	protected List<List<String>> content;
	protected List<String> columnColors;
	protected List<String> lineColors;
	protected List<Boolean> columnRightAligned;
	protected int currentLineIndex = -1;
	protected int currentColumnIndex = -1;

	public TextTable() {
		columnNames = new ArrayList<List<String>>();
		columnColors = new ArrayList<String>();
		content = new ArrayList<List<String>>();
		columnRightAligned = new ArrayList<Boolean>();
		lineColors = new ArrayList<String>();

		// First line contains headers
		columnNames.add(new ArrayList<String>());
	}

	public TextTable(String... columnNames) {
		this();

		for (String columnName : columnNames) {
			addColumn(columnName);
		}
	}

	public boolean hasData() {
		return currentLineIndex >= 0;
	}

	public void addColumn(String columnName) {
		addColumn(columnName, null, false);
	}

	public void addColumn(String columnName, String columnNameLine2, boolean alignRight) {
		addColumn(columnName, columnNameLine2, alignRight, null);
	}

	public void addColumn(String columnName, String columnNameLine2, boolean alignRight, String farbe) {
		columnNames.get(0).add(columnName);
		columnColors.add(farbe);

		// Insert second line
		if (columnNameLine2 != null && columnNames.size() == 1) {
			columnNames.add(new ArrayList<String>());
		}

		if (columnNames.size() > 1) {
			while (columnNames.get(1).size() < columnNames.get(0).size() - 1) {
				columnNames.get(1).add("");
			}
		}

		if (columnNameLine2 != null) {
			columnNames.get(1).add(columnNameLine2);
		} else if (columnNames.size() > 1) {
			columnNames.get(1).add("");
		}

		columnRightAligned.add(alignRight);

		// Adjust existing columns of lines to header length
		for (List<String> line : content) {
			while (line.size() < columnNames.get(0).size()) {
				line.add("");
			}
		}
	}

	public void dropColumn(int index) {
		columnNames.get(0).remove(index);
		columnColors.remove(index);

		if (columnNames.size() > 1) {
			columnNames.get(1).remove(index);
		}

		columnRightAligned.remove(index);

		// Adjust existing columns of lines to header lengths
		for (List<String> line : content) {
			line.remove(index);
		}
	}

	public void setAlignmentForColumn(int columnIndex, boolean alignRight) throws Exception {
		if (columnIndex < 0 || columnIndex >= columnNames.get(0).size()) {
			throw new Exception("Invalid column index: " + columnIndex);
		}

		columnRightAligned.set(columnIndex, alignRight);
	}

	public void startNewLine() {
		startNewLine(null);
	}

	public void startNewLine(String farbe) {
		content.add(new ArrayList<String>());
		lineColors.add(farbe);
		currentLineIndex++;
		currentColumnIndex = 0;

		// Adjust columns of new line to header lengths
		while (content.get(currentLineIndex).size() < columnNames.get(0).size()) {
			content.get(currentLineIndex).add("");
		}
	}

	public void addValueToCurrentLine(int value) throws Exception {
		addValueToCurrentLine(Integer.toString(value));
	}

	public void addValueToCurrentLine(String value) throws Exception {
		if (currentLineIndex == -1) {
			throw new Exception("No line created for extension");
		} else if (content.get(currentLineIndex).size() <= currentColumnIndex) {
			throw new Exception("Tried to write to a non existent line");
		}

		if (value == null) {
			value = "";
		}
		content.get(currentLineIndex).set(currentColumnIndex, value);
		currentColumnIndex++;
	}

	public void setNumericColumnsRightALigned() {
		for (int columnIndex = 0; columnIndex < columnRightAligned.size(); columnIndex++) {
			boolean isNumeric = true;
			for (List<String> line : content) {
				if (Utilities.isNotBlank(line.get(columnIndex)) && !NumberUtilities.isInteger(line.get(columnIndex))) {
					isNumeric = false;
					break;
				}
			}
			columnRightAligned.set(columnIndex, isNumeric);
		}
	}

	@Override
	public String toString() {
		List<StringBuilder> columnNamesLines = new ArrayList<StringBuilder>();
		for (int lineIndex = 0; lineIndex < columnNames.size(); lineIndex++) {
			columnNamesLines.add(new StringBuilder());
		}

		List<StringBuilder> contentLines = new ArrayList<StringBuilder>();
		for (int lineIndex = 0; lineIndex < content.size(); lineIndex++) {
			contentLines.add(new StringBuilder());
		}

		// Iterate over all columns
		for (int columnIndex = 0; columnIndex < columnNames.get(0).size(); columnIndex++) {
			int size = getMaxSizeOfColumn(columnIndex);

			for (int lineIndex = 0; lineIndex < columnNames.size(); lineIndex++) {
				if (columnIndex > 0) {
					columnNamesLines.get(lineIndex).append(" ");
				}

				columnNamesLines.get(lineIndex).append(TextUtilities.trimStringToLengthAlignedLeft(columnNames.get(lineIndex).get(columnIndex), size));
			}

			// Iterate over all lies
			for (int lineIndex = 0; lineIndex < content.size(); lineIndex++) {
				if (columnIndex > 0) {
					contentLines.get(lineIndex).append(" ");
				}

				if (columnRightAligned.get(columnIndex)) {
					contentLines.get(lineIndex).append(TextUtilities.trimStringToLengthAlignedRight(content.get(lineIndex).get(columnIndex), size));
				} else {
					contentLines.get(lineIndex).append(TextUtilities.trimStringToLengthAlignedLeft(content.get(lineIndex).get(columnIndex), size));
				}
			}
		}

		StringBuilder text = new StringBuilder();
		for (int lineIndex = 0; lineIndex < columnNamesLines.size(); lineIndex++) {
			text.append(columnNamesLines.get(lineIndex) + "\n");
		}
		// Remove headerline
		text.append(TextUtilities.repeatString("-", columnNamesLines.get(0).length()) + "\n");
		for (int lineIndex = 0; lineIndex < contentLines.size(); lineIndex++) {
			text.append(contentLines.get(lineIndex) + "\n");
		}

		return text.toString();
	}

	public String toCsvString() {
		StringBuilder csvText = new StringBuilder();

		// Write columnheader first
		// Iterate over all header lines
		for (int lineIndex = 0; lineIndex < columnNames.size(); lineIndex++) {
			// Iterate over all columns
			for (int columnIndex = 0; columnIndex < columnNames.get(0).size(); columnIndex++) {
				csvText.append("\"" + columnNames.get(lineIndex).get(columnIndex) + "\";");
			}
			csvText.append("\n");
		}

		// Write data lines second
		// Iterate over all lines
		for (int lineIndex = 0; lineIndex < content.size(); lineIndex++) {
			// Iterate over all columns
			for (int columnIndex = 0; columnIndex < content.get(0).size(); columnIndex++) {
				csvText.append("\"" + content.get(lineIndex).get(columnIndex) + "\";");
			}
			csvText.append("\n");
		}

		return csvText.toString();
	}

	public String toCsvStringWithMetaData() {
		StringBuilder csvText = new StringBuilder();

		// Write columnheader first
		// Iterate over all header lines
		for (int lineIndex = 0; lineIndex < columnNames.size(); lineIndex++) {
			csvText.append("\"" + METADATA_HEADER + "\";");

			// Iterate over all columns
			for (int columnIndex = 0; columnIndex < columnNames.get(0).size(); columnIndex++) {
				csvText.append("\"" + columnNames.get(lineIndex).get(columnIndex) + "\";");
			}
			csvText.append("\n");
		}

		csvText.append("\"" + METADATA_COLOR + "\";");
		for (int columnIndex = 0; columnIndex < columnNames.get(0).size(); columnIndex++) {
			csvText.append("\"" + columnColors.get(columnIndex) + "\";");
		}
		csvText.append("\n");

		csvText.append("\"" + METADATA_RIGHTALIGNMENT + "\";");
		for (int columnIndex = 0; columnIndex < columnNames.get(0).size(); columnIndex++) {
			csvText.append("\"" + columnRightAligned.get(columnIndex) + "\";");
		}
		csvText.append("\n");

		// Write data lines second
		// Iterate over all lines
		for (int lineIndex = 0; lineIndex < content.size(); lineIndex++) {
			csvText.append("\"" + lineColors.get(lineIndex) + "\";");

			// Iterate over all columns
			for (int columnIndex = 0; columnIndex < content.get(0).size(); columnIndex++) {
				// Remove leading apostrophes
				String cellContentString = content.get(lineIndex).get(columnIndex);
				if (cellContentString != null && cellContentString.startsWith("'") && !cellContentString.endsWith("'")) {
					cellContentString = cellContentString.substring(1);
				}
				csvText.append("\"" + cellContentString + "\";");
			}
			csvText.append("\n");
		}

		return csvText.toString();
	}

	public static String getHtmlHeader(String title) {
		return getHtmlHeader(title, true, null);
	}

	public static String getHtmlHeader(String title, boolean mitZeit) {
		return getHtmlHeader(title, mitZeit, null);
	}

	public static String getHtmlHeader(String title, boolean mitZeit, String inlineCssStyleSheet) {
		return "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0 Transitional//EN\">\n" + "<html>\n" + "<head>\n<title>" + title + "</title>\n"
				+ (Utilities.isEmpty(inlineCssStyleSheet) ? "" : inlineCssStyleSheet) + "</head>\n" + "<body>\n" + "<h2>" + title + "</h2>\n"
				+ (mitZeit ? "<h4>Stand: " + new SimpleDateFormat(DateUtilities.DD_MM_YYYY_HH_MM_SS).format(new GregorianCalendar().getTime()) + "</h4>\n" : "");
	}

	public static String getHtmlFooter() {
		return "<body>\n" + "<html>\n";
	}

	public String toHtmlString(String tableTitle) {
		return toHtmlString(tableTitle, 0);
	}

	public String toHtmlString(String tableTitle, int fontsizeInPt) {
		StringBuilder htmlTableStringBuffer = new StringBuilder();
		htmlTableStringBuffer.append(htmlEncodeString(tableTitle) + "<br><br>\n");
		if (fontsizeInPt > 0) {
			htmlTableStringBuffer.append("<STYLE TYPE=\"text/css\"><!--TD{font-family: Arial; font-size: " + fontsizeInPt + "pt;}---></STYLE>");
		}
		htmlTableStringBuffer.append("<table border='1' cellspacing='1' cellpadding='1'>\n");
		// Alternatively for full width: "<table border='1' width='100%' cellspacing='1' cellpadding='1'>\n");

		int rowcount = 0;

		htmlTableStringBuffer.append("<tr align='center'>");
		// Iterate over all columns
		for (int columnIndex = 0; columnIndex < columnNames.get(0).size(); columnIndex++) {
			if (columnColors.get(columnIndex) == null) {
				htmlTableStringBuffer.append("<td>");
			} else {
				htmlTableStringBuffer.append("<td bgcolor='" + convertXlsColor2HtmlColor(columnColors.get(columnIndex)) + "'>");
			}

			htmlTableStringBuffer.append(htmlEncodeString(columnNames.get(0).get(columnIndex)).replace(" ", "&nbsp;").replace("-", "&#8209;"));
			if (columnNames.size() > 1) {
				htmlTableStringBuffer.append("<br/>" + htmlEncodeString(columnNames.get(1).get(columnIndex)).replace(" ", "&nbsp;").replace("-", "&#8209;"));
			}
			htmlTableStringBuffer.append("</td>");
		}
		htmlTableStringBuffer.append("</tr>\n");

		// Iterate over all lines
		for (int lineIndex = 0; lineIndex < content.size(); lineIndex++) {
			if (lineColors.get(lineIndex) != null) {
				htmlTableStringBuffer.append("<tr align='right' bgcolor='" + lineColors.get(lineIndex) + "'>");
			} else if (rowcount % 2 == 0) {
				htmlTableStringBuffer.append("<tr align='right' bgcolor='cornflowerblue'>");
			} else {
				htmlTableStringBuffer.append("<tr align='right' bgcolor='lightblue'>");
			}

			// Iterate over all columns
			for (int columnIndex = 0; columnIndex < columnNames.get(0).size(); columnIndex++) {
				// Remove leading apostrophes
				String cellContentString = content.get(lineIndex).get(columnIndex);
				if (cellContentString != null && cellContentString.startsWith("'") && !cellContentString.endsWith("'")) {
					cellContentString = cellContentString.substring(1);
				}
				htmlTableStringBuffer.append("<td>" + htmlEncodeString(cellContentString).replace(" ", "&nbsp;").replace("-", "&#8209;") + "</td>");
			}
			htmlTableStringBuffer.append("</tr>\n");

			rowcount++;
		}

		htmlTableStringBuffer.append("</table>\n");
		htmlTableStringBuffer.append("<br>\n");
		htmlTableStringBuffer.append("<br>\n");

		return htmlTableStringBuffer.toString();
	}

	protected int getMaxSizeOfColumn(int index) {
		int maxSize = 0;
		for (List<String> line : columnNames) {
			maxSize = Math.max(line.get(index).length(), maxSize);
		}
		for (List<String> line : content) {
			maxSize = Math.max(line.get(index).length(), maxSize);
		}
		return maxSize;
	}

	public static String convertXlsColor2HtmlColor(String xlsColor) {
		if ("VERY_LIGHT_YELLOW".equalsIgnoreCase(xlsColor)) {
			return "yellow";
		} else if ("LIGHT_GREEN".equalsIgnoreCase(xlsColor)) {
			return "lime";
		} else if ("LAVENDER".equalsIgnoreCase(xlsColor)) {
			return "fuchsia";
		} else if ("TAN".equalsIgnoreCase(xlsColor)) {
			return "fuchsia";
		} else if ("TURQUOISE".equalsIgnoreCase(xlsColor)) {
			return "aqua";
		} else if ("YELLOW".equalsIgnoreCase(xlsColor)) {
			return "yellow";
		} else if ("LIGHT_TURQUOISE".equalsIgnoreCase(xlsColor)) {
			return "aqua";
		} else if ("LIGHT_ORANGE".equalsIgnoreCase(xlsColor)) {
			return "red";
		} else if ("ROSE".equalsIgnoreCase(xlsColor)) {
			return "fuchsia";
		} else if ("WOCHENENDE".equalsIgnoreCase(xlsColor)) {
			// Special color adaptions for Html statistics
			return "coral";
		} else {
			return "white";
		}
	}

	public void loadCsvWithMetadataFile(byte[] csvWithMetadata) throws IOException {
		columnNames.clear();
		content.clear();
		columnColors.clear();
		lineColors.clear();
		columnRightAligned.clear();
		currentLineIndex = -1;
		currentColumnIndex = 0;

		BufferedReader fileReader = null;

		try {
			fileReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(csvWithMetadata), "UTF-8"));
			String fileLine = null;
			while ((fileLine = fileReader.readLine()) != null) {
				String[] lineParts = fileLine.split(";");
				for (int i = 0; i < lineParts.length; i++) {
					if (lineParts[i].startsWith("\"")) {
						lineParts[i] = lineParts[i].substring(1);
					}
					if (lineParts[i].endsWith("\"")) {
						lineParts[i] = lineParts[i].substring(0, lineParts[i].length() - 1);
					}
				}
				if (METADATA_HEADER.equals(lineParts[0])) {
					List<String> headerLine = new ArrayList<String>();
					columnNames.add(headerLine);
					for (int i = 1; i < lineParts.length; i++) {
						headerLine.add(lineParts[i]);
					}
				} else if (METADATA_COLOR.equals(lineParts[0])) {
					for (int i = 1; i < lineParts.length; i++) {
						if (lineParts[i].equalsIgnoreCase("null")) {
							columnColors.add(null);
						} else {
							columnColors.add(lineParts[i]);
						}
					}
				} else if (METADATA_RIGHTALIGNMENT.equals(lineParts[0])) {
					for (int i = 1; i < lineParts.length; i++) {
						if (lineParts[i].equalsIgnoreCase("true")) {
							columnRightAligned.add(true);
						} else {
							columnRightAligned.add(false);
						}
					}
				} else {
					if (lineParts[0].equalsIgnoreCase("null")) {
						lineColors.add(null);
					} else {
						lineColors.add(lineParts[0]);
					}
					List<String> contentLine = new ArrayList<String>();
					content.add(contentLine);
					for (int i = 1; i < lineParts.length; i++) {
						contentLine.add(lineParts[i]);
					}
				}
			}
		} finally {
			if (fileReader != null) {
				try {
					fileReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			fileReader = null;
		}
	}

	public void keepOnlyLastRows(int numberOfLastRows) {
		while (content.size() > numberOfLastRows) {
			content.remove(0);
		}
	}

	public void keepOnlyFirstRows(int numberOfFirstRows) {
		while (content.size() > numberOfFirstRows) {
			content.remove(content.size() - 1);
		}
	}

	public int getHeaderLineCount() {
		return columnNames.size();
	}

	public int getColumnCount() {
		return columnNames.get(0).size();
	}

	public int getDataLineCount() {
		return content.size();
	}

	public String getHeader(int headerLineIndex, int columnIndex) throws Exception {
		if (columnNames.size() <= headerLineIndex || columnNames.get(headerLineIndex).size() <= columnIndex) {
			throw new Exception("Invalid data request at columnheaders line index " + headerLineIndex + " column index " + columnIndex);
		}
		return columnNames.get(headerLineIndex).get(columnIndex);
	}

	public String getHeaderColor(int columnIndex) {
		return columnColors.get(columnIndex);
	}

	public String getLineColor(int lineIndex) {
		return lineColors.get(lineIndex);
	}

	public String getData(int lineIndex, int columnIndex) throws Exception {
		if (content.size() <= lineIndex || content.get(lineIndex).size() <= columnIndex) {
			throw new Exception("Invalid data request at line index " + lineIndex + " column index " + columnIndex);
		}
		return content.get(lineIndex).get(columnIndex);
	}

	public static String htmlEncodeString(String value) {
		return value.replace("&", "&amp;").replace("<", "&lt;").replace("Ä", "&Auml;").replace("ä", "&auml;").replace("Ö", "&Ouml;").replace("ö", "&ouml;").replace("Ü", "&Uuml;")
				.replace("ü", "&uuml;").replace("ß", "&slig;");
	}
}
