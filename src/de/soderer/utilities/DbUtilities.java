package de.soderer.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import de.soderer.utilities.collection.CaseInsensitiveMap;

public class DbUtilities {
	
	/**
	 * In an Oracle DB the statement "SELECT CURRENT_TIMESTAMP FROM DUAL" return this special Oracle type "oracle.sql.TIMESTAMPTZ",
	 * which is not listed in java.sql.Types, but can be read via ResultSet.getTimestamp(i) into a normal java.sql.Timestamp object
	 */
	public static final int ORACLE_TIMESTAMPTZ_TYPECODE = -101;
	
	public enum DbVendor {
		Oracle("oracle.jdbc.OracleDriver", 1521),
		MySQL("com.mysql.jdbc.Driver", 3306),
		PostgreSQL("org.postgresql.Driver", 5432),
		SQLite("org.sqlite.JDBC", 0),
		Derby("org.apache.derby.jdbc.EmbeddedDriver", 0);
		
		public static DbVendor getDbVendorByName(String dbVendorName) throws Exception {
			if ("oracle".equalsIgnoreCase(dbVendorName)) {
				return DbUtilities.DbVendor.Oracle;
			} else if ("mysql".equalsIgnoreCase(dbVendorName)) {
				return DbUtilities.DbVendor.MySQL;
			} else if ("postgres".equalsIgnoreCase(dbVendorName) || "postgresql".equalsIgnoreCase(dbVendorName)) {
				return DbUtilities.DbVendor.PostgreSQL;
			} else if ("sqlite".equalsIgnoreCase(dbVendorName)) {
				return DbUtilities.DbVendor.SQLite;
			} else if ("derby".equalsIgnoreCase(dbVendorName)) {
				return DbUtilities.DbVendor.Derby;
			} else {
				throw new Exception("Unknown db vendor: " + dbVendorName);
			}
		}
		
		private String driverClassName;
		private int defaultPort;
		
		private DbVendor(String driverClassName, int defaultPort) {
			this.driverClassName = driverClassName;
			this.defaultPort = defaultPort;
		}
		
		public String getDriverClassName() {
			return driverClassName;
		}
		
		public int getDefaultPort() {
			return defaultPort;
		}
	}

	public static String generateUrlConnectionString(DbVendor dbVendor, String dbServerHostname, int dbServerPort, String dbName) throws Exception {
		if (DbVendor.Oracle == dbVendor) {
			return "jdbc:oracle:thin:@" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + ":" + dbName;
		} else if (DbVendor.MySQL == dbVendor) {
			return "jdbc:mysql://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName + "?useEncoding=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
		} else if (DbVendor.PostgreSQL == dbVendor) {
			return "jdbc:postgresql://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName;
		} else if (DbVendor.SQLite == dbVendor) {
			return "jdbc:sqlite:" + dbName.replace("~", System.getProperty("user.home"));
		} else if (DbVendor.Derby == dbVendor) {
			return "jdbc:derby:" + dbName.replace("~", System.getProperty("user.home") + ";create=false");
		} else {
			throw new Exception("Unknown db vendor");
		}
	}
	
	public static Connection createConnection(DbVendor dbVendor, String hostname, String dbName, String userName, char[] password) throws Exception {
		if (dbVendor == null) {
			throw new Exception("Unknown db vendor");
		}
		
		Class.forName(dbVendor.getDriverClassName());
	
		if (dbVendor == DbVendor.SQLite) {
			dbName = dbName.replace("~", System.getProperty("user.home"));
			if (!new File(dbName).exists()) {
				throw new Exception("SQLite db file '" + dbName + "' is not available");
			}
			return DriverManager.getConnection(DbUtilities.generateUrlConnectionString(dbVendor, "", 0, dbName));
		} else if (dbVendor == DbVendor.Derby) {
			dbName = dbName.replace("~", System.getProperty("user.home"));
			if (!new File(dbName).exists()) {
				throw new Exception("Derby db file '" + dbName + "' is not available");
			}
			return DriverManager.getConnection(DbUtilities.generateUrlConnectionString(dbVendor, "", 0, dbName));
		} else {
			int port;
			String[] hostParts = hostname.split(":");
			if (hostParts.length == 2) {
				try {
					port = Integer.parseInt(hostParts[1]);
				} catch (Exception e) {
					throw new Exception("Invalid port: " + hostParts[1]);
				}
			} else {
				port = dbVendor.getDefaultPort();
			}
		
			return DriverManager.getConnection(DbUtilities.generateUrlConnectionString(dbVendor, hostParts[0], port, dbName), userName, new String(password));
		}
	}

	public static int readoutTableData(DataSource dataSource, String tableName, OutputStream outputStream, String encoding, char separator, Character stringQuote) throws Exception {
		return readoutData(dataSource, "SELECT * FROM " + tableName, outputStream, encoding, separator, stringQuote);
	}

	public static int readoutData(DataSource dataSource, String statementString, OutputStream outputStream, String encoding, char separator, Character stringQuote) throws Exception {
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		CsvWriter csvWriter = null;
		try {
			connection = dataSource.getConnection();
			statement = connection.createStatement();
			resultSet = statement.executeQuery(statementString);
			csvWriter = new CsvWriter(outputStream, encoding, separator, stringQuote);
			ResultSetMetaData metaData = resultSet.getMetaData();
			// write headers
			List<String> headers = new ArrayList<String>();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				headers.add(metaData.getColumnName(i));
			}
			csvWriter.writeValues(headers);

			// write values
			while (resultSet.next()) {
				List<String> values = new ArrayList<String>();
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					values.add(resultSet.getString(i));
				}
				csvWriter.writeValues(values);
			}

			return csvWriter.getWrittenLines();
		} finally {
			if (resultSet != null) {
				resultSet.close();
			}

			if (statement != null) {
				statement.close();
			}

			if (connection != null) {
				connection.close();
			}

			Utilities.closeQuietly(csvWriter);
		}
	}

	public static String readout(Connection databaseConnection, String statementString, char separator) throws Exception {
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			statement = databaseConnection.createStatement();
			resultSet = statement.executeQuery(statementString);
			StringBuilder tableDataString = new StringBuilder();
			ResultSetMetaData metaData = resultSet.getMetaData();
			// write headers
			List<String> headers = new ArrayList<String>();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				headers.add(metaData.getColumnName(i));
			}
			tableDataString.append(CsvWriter.getCsvLine(separator, '"', headers));
			tableDataString.append("\n");

			// write values
			while (resultSet.next()) {
				List<String> values = new ArrayList<String>();
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					values.add(resultSet.getString(i));
				}
				tableDataString.append(CsvWriter.getCsvLine(separator, '"', values));
				tableDataString.append("\n");
			}

			return tableDataString.toString();
		} finally {
			if (resultSet != null) {
				resultSet.close();
			}

			if (statement != null) {
				statement.close();
			}
		}
	}

	public static String readout(DataSource dataSource, String statementString, char separator) throws Exception {
		Connection connection = null;
		try {
			connection = dataSource.getConnection();

			return readout(connection, statementString, separator);
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}

	public static String readoutTable(DataSource dataSource, String tableName, char separator) throws Exception {
		return readout(dataSource, "SELECT * FROM " + tableName, separator);
	}

	public static String readoutTable(Connection connection, String tableName, char separator) throws Exception {
		return readout(connection, "SELECT * FROM " + tableName, separator);
	}

	public static Map<Integer, Object[]> insertDataInTable(DataSource dataSource, String tableName, String[] tableColumns, List<Object[]> dataSets, boolean commitOnFullSuccessOnly) throws Exception {
		if (Utilities.isBlank(tableName)) {
			throw new Exception("Missing parameter tableName for datainsert");
		}

		Connection connection = null;
		PreparedStatement preparedStatement = null;
		boolean previousAutoCommit = true;

		try {
			connection = dataSource.getConnection();
			previousAutoCommit = connection.getAutoCommit();
			connection.setAutoCommit(false);

			checkTableAndColumnsExist(connection, tableName, tableColumns, true);

			// Insert data
			Map<Integer, Object[]> notInsertedData = new HashMap<Integer, Object[]>();
			String insertStatementString = "INSERT INTO " + tableName + " (" + Utilities.join(tableColumns, ", ") + ") VALUES (" + TextUtilities.repeatString("?", tableColumns.length, ", ") + ")";
			preparedStatement = connection.prepareStatement(insertStatementString);
			boolean hasOpenData = false;
			List<Object[]> currentUncommitedLines = new ArrayList<Object[]>();
			int datasetIndex;
			for (datasetIndex = 0; datasetIndex < dataSets.size(); datasetIndex++) {
				Object[] dataSet = dataSets.get(datasetIndex);
				currentUncommitedLines.add(dataSet);
				hasOpenData = true;

				if (dataSet.length != tableColumns.length) {
					if (!commitOnFullSuccessOnly) {
						notInsertedData.put(datasetIndex, dataSet);
					} else {
						connection.rollback();
						throw new Exception("Error on insert of dataset at index " + datasetIndex + ": invalid number of dataitems");
					}
				} else {
					preparedStatement.clearParameters();
					for (int parameterIndex = 0; parameterIndex < dataSet.length; parameterIndex++) {
						if (dataSet[parameterIndex].getClass() == Date.class) {
							preparedStatement.setObject(parameterIndex + 1, new java.sql.Date(((Date) dataSet[parameterIndex]).getTime()));
						} else {
							preparedStatement.setObject(parameterIndex + 1, dataSet[parameterIndex]);
						}
					}
					preparedStatement.addBatch();

					if ((datasetIndex + 1) % 100 == 0) {
						hasOpenData = false;
						try {
							preparedStatement.executeBatch();
							if (!commitOnFullSuccessOnly) {
								connection.commit();
							}
							currentUncommitedLines.clear();
						} catch (BatchUpdateException bue) {
							if (commitOnFullSuccessOnly) {
								connection.rollback();
								throw new Exception(
										"Error on insert of dataset between index " + (datasetIndex - currentUncommitedLines.size()) + " and index " + datasetIndex + ": " + bue.getMessage());
							} else {
								connection.rollback();
								insertDataInTable(datasetIndex - currentUncommitedLines.size(), connection, preparedStatement, tableColumns, currentUncommitedLines, notInsertedData);
							}
						} catch (Exception e) {
							connection.rollback();
							throw new Exception("Error on insert of dataset between index " + (datasetIndex - currentUncommitedLines.size()) + " and index " + datasetIndex + ": " + e.getMessage());
						}
					}
				}
			}

			if (hasOpenData) {
				hasOpenData = false;
				try {
					preparedStatement.executeBatch();
					if (!commitOnFullSuccessOnly) {
						connection.commit();
					}
					currentUncommitedLines.clear();
				} catch (BatchUpdateException bue) {
					if (commitOnFullSuccessOnly) {
						connection.rollback();
						throw new Exception("Error on insert of dataset between index " + (datasetIndex - currentUncommitedLines.size()) + " and index " + datasetIndex + ": " + bue.getMessage());
					} else {
						connection.rollback();
						insertDataInTable(datasetIndex - currentUncommitedLines.size(), connection, preparedStatement, tableColumns, currentUncommitedLines, notInsertedData);
					}
				} catch (Exception e) {
					connection.rollback();
					throw new Exception("Error on insert of dataset between index " + (datasetIndex - currentUncommitedLines.size()) + " and index " + datasetIndex + ": " + e.getMessage());
				}
			}

			if (commitOnFullSuccessOnly && notInsertedData.size() == 0) {
				connection.commit();
			} else {
				connection.rollback();
			}

			return notInsertedData;
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}

			if (connection != null) {
				try {
					connection.setAutoCommit(previousAutoCommit);
				} catch (SQLException e) {
					// fo nothing
				}

				connection.rollback();
				connection.close();
			}
		}
	}

	private static void insertDataInTable(int offsetIndex, Connection connection, PreparedStatement preparedStatement, String[] columnMapping, List<Object[]> data,
			Map<Integer, Object[]> notInsertedData) throws Exception {
		int dataLineIndex = offsetIndex;
		for (Object[] dataLine : data) {
			dataLineIndex++;
			if (dataLine.length != columnMapping.length) {
				notInsertedData.put(dataLineIndex, dataLine);
			} else {
				int parameterIndex = 1;
				for (int csvValueIndex = 0; csvValueIndex < dataLine.length; csvValueIndex++) {
					if (columnMapping[csvValueIndex] != null) {
						if (dataLine[csvValueIndex].getClass() == Date.class) {
							preparedStatement.setObject(parameterIndex++, new java.sql.Date(((Date) dataLine[csvValueIndex]).getTime()));
						} else {
							preparedStatement.setObject(parameterIndex++, dataLine[csvValueIndex]);
						}
					}
				}

				try {
					preparedStatement.execute();
					connection.commit();
				} catch (Exception e) {
					notInsertedData.put(dataLineIndex, dataLine);
					connection.rollback();
				}
			}
		}
	}

	public static Map<Integer, Tuple<List<String>, String>> insertCsvFileInTable(DataSource dataSource, String tableName, String[] columnMapping, File csvFile, String encoding,
			boolean commitOnFullSuccessOnly) throws Exception {
		FileInputStream fileInputStream = null;
		try {
			fileInputStream = new FileInputStream(csvFile);
			return insertCsvFileInTable(dataSource, tableName, columnMapping, fileInputStream, encoding, false, commitOnFullSuccessOnly);
		} finally {
			Utilities.closeQuietly(fileInputStream);
		}
	}

	public static Map<Integer, Tuple<List<String>, String>> insertCsvFileInTable(DataSource dataSource, String tableName, String[] columnMapping, File csvFile, String encoding,
			boolean fillMissingTrailingColumnsWithNull, boolean commitOnFullSuccessOnly, boolean containsHeadersInFirstRow) throws Exception {
		FileInputStream fileInputStream = null;
		try {
			fileInputStream = new FileInputStream(csvFile);
			return insertCsvFileInTable(dataSource, tableName, columnMapping, fileInputStream, encoding, fillMissingTrailingColumnsWithNull, commitOnFullSuccessOnly, containsHeadersInFirstRow);
		} finally {
			Utilities.closeQuietly(fileInputStream);
		}
	}

	public static Map<Integer, Tuple<List<String>, String>> insertCsvFileInTable(DataSource dataSource, String tableName, String[] columnMapping, FileInputStream csvFileInputStream, String encoding,
			boolean commitOnFullSuccessOnly, boolean containsHeadersInFirstRow) throws Exception {
		return insertCsvFileInTable(dataSource, tableName, columnMapping, csvFileInputStream, encoding, false, commitOnFullSuccessOnly, containsHeadersInFirstRow);
	}

	public static Map<Integer, Tuple<List<String>, String>> insertCsvFileInTable(DataSource dataSource, String tableName, String[] columnMapping, FileInputStream csvFileInputStream, String encoding,
			boolean fillMissingTrailingColumnsWithNull, boolean commitOnFullSuccessOnly, boolean containsHeadersInFirstRow) throws Exception {
		return insertCsvFileInTable(dataSource, tableName, columnMapping, null, csvFileInputStream, encoding, fillMissingTrailingColumnsWithNull, commitOnFullSuccessOnly, containsHeadersInFirstRow);
	}

	public static Map<Integer, Tuple<List<String>, String>> insertCsvFileInTable(DataSource dataSource, String tableName, String[] columnMapping, String[] columnDataFormats,
			InputStream csvInputStream, String encoding, boolean fillMissingTrailingColumnsWithNull, boolean commitOnFullSuccessOnly, boolean containsHeadersInFirstRow) throws Exception {
		return insertCsvDataInTable(dataSource, tableName, columnMapping, columnDataFormats, csvInputStream, encoding, ';', null, fillMissingTrailingColumnsWithNull, commitOnFullSuccessOnly,
				containsHeadersInFirstRow);
	}

	public static Map<Integer, Tuple<List<String>, String>> insertCsvDataInTable(DataSource dataSource, String tableName, String[] columnMapping, String[] columnDataFormats,
			InputStream csvInputStream, String encoding, char separatorChar, Character stringQuoteChar, boolean fillMissingTrailingColumnsWithNull, boolean commitOnFullSuccessOnly,
			boolean containsHeadersInFirstRow) throws Exception {
		if (Utilities.isBlank(tableName)) {
			throw new Exception("Missing parameter tableName for datainsert");
		}

		Connection connection = null;
		boolean previousAutoCommit = true;
		PreparedStatement preparedStatement = null;
		Statement statement = null;
		ResultSet resultSet = null;

		CsvReader csvReader = null;

		try {
			connection = dataSource.getConnection();
			previousAutoCommit = connection.getAutoCommit();
			connection.setAutoCommit(false);
			statement = connection.createStatement();

			csvReader = new CsvReader(csvInputStream, encoding, separatorChar, stringQuoteChar);
			csvReader.setFillMissingTrailingColumnsWithNull(fillMissingTrailingColumnsWithNull);

			// First line may contain headers
			List<String> csvLine;
			if (containsHeadersInFirstRow) {
				csvLine = csvReader.readNextCsvLine();
				if (columnMapping == null) {
					columnMapping = csvLine.toArray(new String[0]);
				}
			}

			checkTableAndColumnsExist(connection, tableName, columnMapping, true);

			List<String> dbColumns = new ArrayList<String>();
			for (String column : columnMapping) {
				if (column != null) {
					dbColumns.add(column);
				}
			}

			Map<Integer, Tuple<List<String>, String>> notInsertedData = new HashMap<Integer, Tuple<List<String>, String>>();
			String insertStatementString = "INSERT INTO " + tableName + " (" + Utilities.join(dbColumns, ", ") + ") VALUES (" + TextUtilities.repeatString("?", dbColumns.size(), ", ") + ")";
			preparedStatement = connection.prepareStatement(insertStatementString);

			// Read and insert data
			int csvLineIndex = 1; // index obeys headerline => real lineindex in csv-file
			boolean hasOpenData = false;
			List<List<String>> currentUncommitedLines = new ArrayList<List<String>>();
			while ((csvLine = csvReader.readNextCsvLine()) != null) {
				csvLineIndex++;
				currentUncommitedLines.add(csvLine);
				hasOpenData = true;

				if (csvLine.size() != columnMapping.length) {
					if (!commitOnFullSuccessOnly) {
						notInsertedData.put(csvLineIndex, new Tuple<List<String>, String>(csvLine, "Not enough values"));
					} else {
						connection.rollback();
						throw new Exception("Error on insert of dataset at line " + csvLineIndex + ": invalid number of dataitems");
					}
				} else {
					int parameterIndex = 1;
					for (int csvValueIndex = 0; csvValueIndex < csvLine.size(); csvValueIndex++) {
						if (columnMapping[csvValueIndex] != null) {
							String value = csvLine.get(csvValueIndex);
							if (columnDataFormats != null && Utilities.isNotBlank(columnDataFormats[csvValueIndex])) {
								if (".".equalsIgnoreCase(columnDataFormats[csvValueIndex])) {
									value = value.replace(",", "");
									preparedStatement.setObject(parameterIndex++, value);
								} else if (",".equalsIgnoreCase(columnDataFormats[csvValueIndex])) {
									value = value.replace(".", "");
									value = value.replace(",", ".");
									preparedStatement.setObject(parameterIndex++, value);
								} else {
									preparedStatement.setObject(parameterIndex++, new java.sql.Date(new SimpleDateFormat(columnDataFormats[csvValueIndex]).parse(value).getTime()));
								}
							} else {
								preparedStatement.setString(parameterIndex++, value);
							}
						}
					}
					preparedStatement.addBatch();

					if (csvLineIndex % 100 == 0) {
						hasOpenData = false;
						try {
							preparedStatement.executeBatch();
							if (!commitOnFullSuccessOnly) {
								connection.commit();
							}
							currentUncommitedLines.clear();
						} catch (BatchUpdateException bue) {
							if (commitOnFullSuccessOnly) {
								connection.rollback();
								throw new Exception(
										"Error on insert of dataset between line " + (csvLineIndex - currentUncommitedLines.size()) + " and line " + csvLineIndex + ": " + bue.getMessage());
							} else {
								connection.rollback();
								insertCsvDataInTable(csvLineIndex - currentUncommitedLines.size(), connection, preparedStatement, columnMapping, columnDataFormats, currentUncommitedLines,
										notInsertedData);
							}
						} catch (Exception e) {
							if (!commitOnFullSuccessOnly) {
								notInsertedData.put(csvLineIndex, new Tuple<List<String>, String>(csvLine, e.getMessage()));
								connection.rollback();
							} else {
								connection.rollback();
								throw new Exception("Error on insert of dataset at line " + csvLineIndex + ": " + e.getMessage());
							}
						}
					}
				}
			}

			if (hasOpenData) {
				hasOpenData = false;
				try {
					preparedStatement.executeBatch();
					if (!commitOnFullSuccessOnly) {
						connection.commit();
					}
					currentUncommitedLines.clear();
				} catch (BatchUpdateException bue) {
					if (commitOnFullSuccessOnly) {
						connection.rollback();
						throw new Exception("Error on insert of dataset between line " + (csvLineIndex - currentUncommitedLines.size()) + " and line " + csvLineIndex + ": " + bue.getMessage());
					} else {
						connection.rollback();
						insertCsvDataInTable(csvLineIndex - currentUncommitedLines.size(), connection, preparedStatement, columnMapping, columnDataFormats, currentUncommitedLines, notInsertedData);
					}
				} catch (Exception e) {
					connection.rollback();
					throw new Exception("Error on insert of dataset between line " + (csvLineIndex - currentUncommitedLines.size()) + " and line " + csvLineIndex + ": " + e.getMessage());
				}
			}

			if (commitOnFullSuccessOnly && notInsertedData.size() == 0) {
				connection.commit();
			} else {
				connection.rollback();
			}

			return notInsertedData;
		} finally {
			if (csvReader != null) {
				csvReader.close();
			}

			if (resultSet != null) {
				resultSet.close();
			}

			if (statement != null) {
				statement.close();
			}

			if (preparedStatement != null) {
				preparedStatement.close();
			}

			if (connection != null) {
				try {
					connection.setAutoCommit(previousAutoCommit);
				} catch (SQLException e) {
					// fo nothing
				}

				connection.rollback();
				connection.close();
			}
		}
	}

	public static void insertCsvDataInTable(int offsetIndex, Connection connection, PreparedStatement preparedStatement, String[] columnMapping, String[] columnDataFormats, List<List<String>> data,
			Map<Integer, Tuple<List<String>, String>> notInsertedData) throws Exception {
		int csvLineIndex = offsetIndex;
		for (List<String> csvLine : data) {
			csvLineIndex++;
			if (csvLine.size() != columnMapping.length) {
				notInsertedData.put(csvLineIndex, new Tuple<List<String>, String>(csvLine, "Not enough values"));
			} else {
				int parameterIndex = 1;
				for (int csvValueIndex = 0; csvValueIndex < csvLine.size(); csvValueIndex++) {
					String value = csvLine.get(csvValueIndex);
					if (columnDataFormats != null && Utilities.isNotBlank(columnDataFormats[csvValueIndex])) {
						if (".".equalsIgnoreCase(columnDataFormats[csvValueIndex])) {
							value = value.replace(",", "");
							preparedStatement.setObject(parameterIndex++, value);
						} else if (",".equalsIgnoreCase(columnDataFormats[csvValueIndex])) {
							value = value.replace(".", "");
							value = value.replace(",", ".");
							preparedStatement.setObject(parameterIndex++, value);
						} else {
							preparedStatement.setObject(parameterIndex++, new java.sql.Date(new SimpleDateFormat(columnDataFormats[csvValueIndex]).parse(value).getTime()));
						}
					} else {
						preparedStatement.setString(parameterIndex++, value);
					}
				}

				try {
					preparedStatement.execute();
					connection.commit();
				} catch (Exception e) {
					notInsertedData.put(csvLineIndex, new Tuple<List<String>, String>(csvLine, e.getMessage()));
					connection.rollback();
				}
			}
		}
	}

	public static DbVendor getDbVendor(DataSource dataSource) throws Exception {
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
			return getDbVendor(connection);
		} catch (SQLException e) {
			throw new RuntimeException("Cannot check db vendor: " + e.getMessage(), e);
		} finally {
			closeQuietly(connection);
		}
	}

	public static DbVendor getDbVendor(Connection connection) throws Exception {
		try {
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			if (databaseMetaData != null) {
				String productName = databaseMetaData.getDatabaseProductName();
				if (productName != null && productName.toLowerCase().contains("oracle")) {
					return DbVendor.Oracle;
				} else if (productName != null && productName.toLowerCase().contains("mysql")) {
					return DbVendor.MySQL;
				} else if (productName != null && productName.toLowerCase().contains("postgres")) {
					return DbVendor.PostgreSQL;
				} else if (productName != null && productName.toLowerCase().contains("sqlite")) {
					return DbVendor.SQLite;
				} else if (productName != null && productName.toLowerCase().contains("derby")) {
					return DbVendor.Derby;
				} else {
					throw new Exception("Unknown db vendor: " + productName);
				}
			} else {
				throw new Exception("Undetectable db vendor");
			}
		} catch (SQLException e) {
			throw new Exception("Error while detecting db vendor: " + e.getMessage(), e);
		}
	}

	public static String getDbUrl(DataSource dataSource) throws SQLException {
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
			return getDbUrl(connection);
		} finally {
			closeQuietly(connection);
		}
	}

	public static String getDbUrl(Connection connection) {
		try {
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			if (databaseMetaData != null) {
				return databaseMetaData.getURL();
			} else {
				return null;
			}
		} catch (SQLException e) {
			return null;
		}
	}

	public static boolean checkTableAndColumnsExist(Connection connection, String tableName, String[] columns) throws Exception {
		return checkTableAndColumnsExist(connection, tableName, columns, false);
	}

	public static boolean checkTableAndColumnsExist(DataSource dataSource, String tableName, String[] columns, boolean throwExceptionOnError) throws Exception {
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
			return checkTableAndColumnsExist(connection, tableName, columns, throwExceptionOnError);
		} finally {
			closeQuietly(connection);
		}
	}

	public static boolean checkTableAndColumnsExist(Connection connection, String tableName, String[] columns, boolean throwExceptionOnError) throws Exception {
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			statement = connection.createStatement();

			// Check if table exists
			try {
				resultSet = statement.executeQuery("SELECT * FROM " + tableName + " WHERE 1 = 0");
			} catch (Exception e) {
				if (throwExceptionOnError) {
					throw new Exception("Table '" + tableName + "' does not exist");
				} else {
					return false;
				}
			}

			// Check if all needed columns exist
			Set<String> dbTableColumns = new HashSet<String>();
			ResultSetMetaData metaData = resultSet.getMetaData();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				dbTableColumns.add(metaData.getColumnName(i).toUpperCase());
			}
			if (columns != null) {
				for (String column : columns) {
					if (column != null && !dbTableColumns.contains(column.toUpperCase())) {
						if (throwExceptionOnError) {
							throw new Exception("Column '" + column + "' does not exist in table '" + tableName + "'");
						} else {
							return false;
						}
					}
				}
			}
			return true;
		} finally {
			closeQuietly(resultSet);
			resultSet = null;
			closeQuietly(statement);
			statement = null;
		}
	}

	public static String callStoredProcedureWithDbmsOutput(Connection connection, String procedureName, Object... parameters) throws SQLException {
		CallableStatement callableStatement = null;
		try {
			callableStatement = connection.prepareCall("begin dbms_output.enable(:1); end;");
			callableStatement.setLong(1, 10000);
			callableStatement.executeUpdate();
			callableStatement.close();
			callableStatement = null;

			if (parameters != null) {
				callableStatement = connection.prepareCall("{call " + procedureName + "(" + TextUtilities.repeatString("?", parameters.length, ", ") + ")}");
				for (int i = 0; i < parameters.length; i++) {
					if (parameters[i].getClass() == Date.class) {
						parameters[i] = new java.sql.Date(((Date) parameters[i]).getTime());
					}
				}
				for (int i = 0; i < parameters.length; i++) {
					callableStatement.setObject(i + 1, parameters[i]);
				}
			} else {
				callableStatement = connection.prepareCall("{call " + procedureName + "()}");
			}
			callableStatement.execute();
			callableStatement.close();
			callableStatement = null;

			callableStatement = connection.prepareCall("declare " + "    l_line varchar2(255); " + "    l_done number; " + "    l_buffer long; " + "begin " + "  loop "
					+ "    exit when length(l_buffer)+255 > :maxbytes OR l_done = 1; " + "    dbms_output.get_line( l_line, l_done ); " + "    l_buffer := l_buffer || l_line || chr(10); "
					+ "  end loop; " + " :done := l_done; " + " :buffer := l_buffer; " + "end;");

			callableStatement.registerOutParameter(2, Types.INTEGER);
			callableStatement.registerOutParameter(3, Types.VARCHAR);
			StringBuffer dbmsOutput = new StringBuffer(1024);
			while (true) {
				callableStatement.setInt(1, 32000);
				callableStatement.executeUpdate();
				dbmsOutput.append(callableStatement.getString(3).trim());
				if (callableStatement.getInt(2) == 1) {
					break;
				}
			}
			callableStatement.close();
			callableStatement = null;

			callableStatement = connection.prepareCall("begin dbms_output.disable; end;");
			callableStatement.executeUpdate();
			callableStatement.close();
			callableStatement = null;

			return dbmsOutput.toString();
		} finally {
			closeQuietly(callableStatement);
		}
	}

	public static void closeQuietly(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				// do nothing
			}
		}
	}

	public static void closeQuietly(Statement statement) {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				// do nothing
			}
		}
	}

	public static void closeQuietly(ResultSet resultSet) {
		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (SQLException e) {
				// do nothing
			}
		}
	}

	public static String getResultAsTextTable(DataSource datasource, String selectString) throws Exception {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			connection = datasource.getConnection();
			preparedStatement = connection.prepareStatement(selectString);
			resultSet = preparedStatement.executeQuery();
			TextTable textTable = new TextTable();
			for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
				textTable.addColumn(resultSet.getMetaData().getColumnName(columnIndex));
			}
			while (resultSet.next()) {
				textTable.startNewLine();
				for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
					if (resultSet.getString(columnIndex) != null) {
						textTable.addValueToCurrentLine(resultSet.getString(columnIndex));
					} else {
						textTable.addValueToCurrentLine("<null>");
					}
				}
			}
			return textTable.toString();
		} finally {
			closeQuietly(resultSet);
			closeQuietly(preparedStatement);
			closeQuietly(connection);
		}
	}

	public static List<String> getColumnNames(DataSource dataSource, String tableName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getColumnNames");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnNames");
		} else {
			Connection connection = null;
			Statement statement = null;
			ResultSet resultSet = null;
			try {
				connection = dataSource.getConnection();
				statement = connection.createStatement();
				String sql = "SELECT * FROM " + getSQLSafeString(tableName) + " WHERE 1 = 0";
				resultSet = statement.executeQuery(sql);
				List<String> columnNamesList = new ArrayList<String>();
				for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
					columnNamesList.add(resultSet.getMetaData().getColumnName(i));
				}
				return columnNamesList;
			} finally {
				closeQuietly(resultSet);
				closeQuietly(statement);
				closeQuietly(connection);
			}
		}
	}

	public static DbColumnType getColumnDataType(Connection connection, String tableName, String columnName) throws Exception {
		if (connection == null) {
			throw new Exception("Invalid empty connection for getColumnDataType");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnDataType");
		} else if (Utilities.isBlank(columnName)) {
			throw new Exception("Invalid empty columnName for getColumnDataType");
		} else {
			PreparedStatement preparedStatement = null;
			ResultSet resultSet = null;
			try {
				int characterLength;
				int numericPrecision;
				int numericScale;
				boolean isNullable;
				DbVendor dbVendor = getDbVendor(connection);
				if (DbVendor.Oracle == dbVendor) {
					// Watchout: Oracle's timestamp datatype is "TIMESTAMP(6)", so remove the bracket value
					String sql = "SELECT NVL(substr(data_type, 1, instr(data_type, '(') - 1), data_type) as data_type, data_length, data_precision, data_scale, nullable FROM user_tab_columns WHERE lower(table_name) = lower(?) AND lower(column_name) = lower(?)";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName);
					preparedStatement.setString(2, columnName);
					resultSet = preparedStatement.executeQuery();

					if (resultSet.next()) {
						characterLength = resultSet.getInt("data_length");
						if (resultSet.wasNull()) {
							characterLength = -1;
						}
						numericPrecision = resultSet.getInt("data_precision");
						if (resultSet.wasNull()) {
							numericPrecision = -1;
						}
						numericScale = resultSet.getInt("data_scale");
						if (resultSet.wasNull()) {
							numericScale = -1;
						}
						isNullable = resultSet.getString("nullable").equalsIgnoreCase("y");

						return new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable);
					} else {
						return null;
					}
				} else if (DbVendor.MySQL == dbVendor) {
					String sql = "SELECT data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable FROM information_schema.columns WHERE table_schema = schema() AND lower(table_name) = lower(?) AND lower(column_name) = lower(?)";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName);
					preparedStatement.setString(2, columnName);
					resultSet = preparedStatement.executeQuery();

					if (resultSet.next()) {
						characterLength = resultSet.getInt("character_maximum_length");
						if (resultSet.wasNull()) {
							characterLength = -1;
						}
						numericPrecision = resultSet.getInt("numeric_precision");
						if (resultSet.wasNull()) {
							numericPrecision = -1;
						}
						numericScale = resultSet.getInt("numeric_scale");
						if (resultSet.wasNull()) {
							numericScale = -1;
						}
						isNullable = resultSet.getString("is_nullable").equalsIgnoreCase("yes");

						return new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable);
					} else {
						return null;
					}
				} else {
					throw new Exception("Unsupported db vendor");
				}
			} catch (Exception e) {
				return null;
			} finally {
				closeQuietly(resultSet);
				closeQuietly(preparedStatement);
			}
		}
	}

	public static DbColumnType getColumnDataType(DataSource dataSource, String tableName, String columnName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getColumnDataType");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnDataType");
		} else if (Utilities.isBlank(columnName)) {
			throw new Exception("Invalid empty columnName for getColumnDataType");
		} else {
			Connection connection = null;
			try {
				connection = dataSource.getConnection();
				return getColumnDataType(connection, tableName, columnName);
			} catch (Exception e) {
				return null;
			} finally {
				closeQuietly(connection);
			}
		}
	}

	public static CaseInsensitiveMap<DbColumnType> getColumnDataTypes(DataSource dataSource, String tableName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getColumnDataTypes");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnDataTypes");
		} else {
			Connection connection = null;
			PreparedStatement preparedStatement = null;
			ResultSet resultSet = null;
			try {
				CaseInsensitiveMap<DbColumnType> returnMap = new CaseInsensitiveMap<DbColumnType>();
				connection = dataSource.getConnection();
				DbVendor dbVendor = getDbVendor(connection);
				if (DbVendor.Oracle == dbVendor) {
					// Watchout: Oracle's timestamp datatype is "TIMESTAMP(6)", so remove the bracket value
					String sql = "SELECT column_name, NVL(substr(data_type, 1, instr(data_type, '(') - 1), data_type) as data_type, data_length, data_precision, data_scale, nullable FROM user_tab_columns WHERE lower(table_name) = lower(?)";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName);
					resultSet = preparedStatement.executeQuery();
					while (resultSet.next()) {
						int characterLength = resultSet.getInt("data_length");
						if (resultSet.wasNull()) {
							characterLength = -1;
						}
						int numericPrecision = resultSet.getInt("data_precision");
						if (resultSet.wasNull()) {
							numericPrecision = -1;
						}
						int numericScale = resultSet.getInt("data_scale");
						if (resultSet.wasNull()) {
							numericScale = -1;
						}
						boolean isNullable = resultSet.getString("nullable").equalsIgnoreCase("y");

						returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable));
					}
				} else if (DbVendor.MySQL == dbVendor) {
					String sql = "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable FROM information_schema.columns WHERE table_schema = schema() AND lower(table_name) = lower(?)";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName);
					resultSet = preparedStatement.executeQuery();
					while (resultSet.next()) {
						int characterLength = resultSet.getInt("character_maximum_length");
						if (resultSet.wasNull()) {
							characterLength = -1;
						}
						int numericPrecision = resultSet.getInt("numeric_precision");
						if (resultSet.wasNull()) {
							numericPrecision = -1;
						}
						int numericScale = resultSet.getInt("numeric_scale");
						if (resultSet.wasNull()) {
							numericScale = -1;
						}
						boolean isNullable = resultSet.getString("is_nullable").equalsIgnoreCase("yes");

						returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable));
					}
				} else {
					throw new Exception("Unsupported db vendor");
				}
				return returnMap;
			} catch (Exception e) {
				throw e;
			} finally {
				closeQuietly(resultSet);
				closeQuietly(preparedStatement);
				closeQuietly(connection);
			}
		}
	}

	public static int getColumnCount(DataSource dataSource, String tableName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getColumnCount");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnCount");
		} else {
			Connection connection = null;
			try {
				connection = dataSource.getConnection();
				return getColumnCount(connection, tableName);
			} finally {
				closeQuietly(connection);
			}
		}
	}

	public static int getColumnCount(Connection connection, String tableName) throws Exception {
		if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnCount");
		} else {
			Statement statement = null;
			ResultSet resultSet = null;
			try {
				statement = connection.createStatement();
				String sql = "SELECT * FROM " + getSQLSafeString(tableName) + " WHERE 1 = 0";
				resultSet = statement.executeQuery(sql);
				return resultSet.getMetaData().getColumnCount();
			} finally {
				closeQuietly(resultSet);
				closeQuietly(statement);
			}
		}
	}

	public static int getTableEntriesCount(Connection connection, String tableName) throws Exception {
		if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getTableEntriesNumber");
		} else {
			Statement statement = null;
			ResultSet resultSet = null;
			try {
				statement = connection.createStatement();
				String sql = "SELECT COUNT(*) FROM " + getSQLSafeString(tableName);
				resultSet = statement.executeQuery(sql);
				if (resultSet.next()) {
					return resultSet.getInt(1);
				} else {
					return 0;
				}
			} finally {
				closeQuietly(resultSet);
				closeQuietly(statement);
			}
		}
	}

	public static boolean containsColumnName(DataSource dataSource, String tableName, String columnName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for containsColumnName");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for containsColumnName");
		} else if (Utilities.isBlank(columnName)) {
			throw new Exception("Invalid empty columnName for containsColumnName");
		} else {
			Connection connection = null;
			Statement statement = null;
			ResultSet resultSet = null;
			try {
				connection = dataSource.getConnection();
				statement = connection.createStatement();
				String sql = "SELECT * FROM " + getSQLSafeString(tableName) + " WHERE 1 = 0";
				resultSet = statement.executeQuery(sql);
				for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
					if (resultSet.getMetaData().getColumnName(columnIndex).equalsIgnoreCase(columnName.trim())) {
						return true;
					}
				}
				return false;
			} finally {
				closeQuietly(resultSet);
				closeQuietly(statement);
				closeQuietly(connection);
			}
		}
	}

	public static int getColumnDataTypeLength(DataSource dataSource, String tableName, String columnName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getDefaultValueOf");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getDefaultValueOf");
		} else if (Utilities.isBlank(columnName)) {
			throw new Exception("Invalid empty columnName for getDefaultValueOf");
		} else {
			Connection connection = null;
			PreparedStatement preparedStatement = null;
			ResultSet resultSet = null;
			try {
				connection = dataSource.getConnection();
				String sql;
				DbVendor dbVendor = getDbVendor(connection);
				if (DbVendor.Oracle == dbVendor) {
					sql = "SELECT data_length FROM user_tab_cols WHERE table_name = ? AND column_name = ?";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName.toUpperCase());
					preparedStatement.setString(2, columnName.toUpperCase());
				} else {
					sql = "SELECT character_maximum_length FROM information_schema.columns WHERE table_schema = (SELECT schema()) AND table_name = ? AND column_name = ?";
					preparedStatement = connection.prepareStatement(sql);
					preparedStatement.setString(1, tableName);
					preparedStatement.setString(2, columnName);
				}
				resultSet = preparedStatement.executeQuery();
				if (resultSet.next()) {
					int returnValue = resultSet.getInt(1);
					if (resultSet.next()) {
						throw new Exception("Cannot retrieve column datatypelength");
					} else {
						return returnValue;
					}
				} else {
					throw new Exception("Cannot retrieve column datatypelength");
				}
			} finally {
				closeQuietly(resultSet);
				closeQuietly(preparedStatement);
				closeQuietly(connection);
			}
		}
	}

	public static String getColumnDefaultValue(DataSource dataSource, String tableName, String columnName) throws Exception {
		try {
			if (dataSource == null) {
				throw new Exception("Invalid empty dataSource for getDefaultValueOf");
			} else if (Utilities.isBlank(tableName)) {
				throw new Exception("Invalid empty tableName for getDefaultValueOf");
			} else if (Utilities.isBlank(columnName)) {
				throw new Exception("Invalid empty columnName for getDefaultValueOf");
			} else {
				Connection connection = null;
				PreparedStatement preparedStatement = null;
				ResultSet resultSet = null;
				try {
					connection = dataSource.getConnection();
					String sql;
					DbVendor dbVendor = getDbVendor(connection);
					if (DbVendor.Oracle == dbVendor) {
						sql = "SELECT data_default FROM user_tab_cols WHERE table_name = ? AND column_name = ?";
						preparedStatement = connection.prepareStatement(sql);
						preparedStatement.setString(1, tableName.toUpperCase());
						preparedStatement.setString(2, columnName.toUpperCase());
						resultSet = preparedStatement.executeQuery();
						if (resultSet.next()) {
							String defaultvalue = resultSet.getString(1);
							String returnValue;
							if (defaultvalue == null || "null".equalsIgnoreCase(defaultvalue)) {
								returnValue = null;
							} else if (defaultvalue.startsWith("'") && defaultvalue.endsWith("'")) {
								returnValue = defaultvalue.substring(1, defaultvalue.length() - 1);
							} else {
								returnValue = defaultvalue;
							}
							if (resultSet.next()) {
								throw new Exception("Cannot retrieve column datatype");
							} else {
								return returnValue;
							}
						} else {
							throw new Exception("Cannot retrieve column datatype");
						}
					} else {
						sql = "SELECT column_default FROM information_schema.columns WHERE table_schema = (SELECT schema()) AND table_name = ? AND column_name = ?";
						preparedStatement = connection.prepareStatement(sql);
						preparedStatement.setString(1, tableName);
						preparedStatement.setString(2, columnName);
						resultSet = preparedStatement.executeQuery();
						if (resultSet.next()) {
							String returnValue = resultSet.getString(1);
							if (resultSet.next()) {
								throw new Exception("Cannot retrieve column datatype");
							} else {
								return returnValue;
							}
						} else {
							throw new Exception("Cannot retrieve column datatype");
						}
					}

				} finally {
					closeQuietly(resultSet);
					closeQuietly(preparedStatement);
					closeQuietly(connection);
				}
			}
		} catch (Exception e) {
			throw e;
		}
	}

	public static String getDateDefaultValue(DataSource dataSource, String fieldDefault) throws Exception {
		DbVendor dbVendor = getDbVendor(dataSource);
		if (fieldDefault.equalsIgnoreCase("sysdate") || fieldDefault.equalsIgnoreCase("sysdate()") || fieldDefault.equalsIgnoreCase("current_timestamp")) {
			if (dbVendor == DbVendor.Oracle) {
				return "current_timestamp";
			} else if (dbVendor == DbVendor.MySQL) {
				return "sysdate()";
			} else {
				throw new Exception("Unsupported db vendor");
			}
		} else {
			if (dbVendor == DbVendor.Oracle) {
				return "to_date('" + fieldDefault + "', 'DD.MM.YYYY')";
			} else if (dbVendor == DbVendor.MySQL) {
				return "'" + fieldDefault + "'";
			} else {
				throw new Exception("Unsupported db vendor");
			}
		}
	}

	public static boolean addColumnToDbTable(DataSource dataSource, String tablename, String fieldname, String fieldType, int length, String fieldDefault, boolean notNull) throws Exception {
		if (Utilities.isBlank(fieldname)) {
			return false;
		} else if (!tablename.equalsIgnoreCase(getSQLSafeString(tablename))) {
			return false;
		} else if (Utilities.isBlank(fieldname)) {
			return false;
		} else if (!fieldname.equalsIgnoreCase(getSQLSafeString(fieldname))) {
			return false;
		} else if (Utilities.isBlank(fieldType)) {
			return false;
		} else if (containsColumnName(dataSource, tablename, fieldname)) {
			return false;
		} else {
			fieldType = fieldType.toUpperCase().trim();

			String addColumnStatement = "ALTER TABLE " + tablename + " ADD (" + fieldname.toLowerCase() + " " + fieldType;
			if (fieldType.startsWith("VARCHAR")) {
				if (length <= 0) {
					length = 100;
				}
				addColumnStatement += "(" + length + ")";
			}

			// Default Value
			if (Utilities.isNotEmpty(fieldDefault)) {
				if (fieldType.startsWith("VARCHAR")) {
					addColumnStatement += " DEFAULT '" + fieldDefault + "'";
				} else if (fieldType.equalsIgnoreCase("DATE")) {
					addColumnStatement += " DEFAULT " + getDateDefaultValue(dataSource, fieldDefault);
				} else {
					addColumnStatement += " DEFAULT " + fieldDefault;
				}
			}

			// Maybe null
			if (notNull) {
				addColumnStatement += " NOT NULL";
			}

			addColumnStatement += ")";

			Connection connection = null;
			Statement statement = null;
			try {
				connection = dataSource.getConnection();
				statement = connection.createStatement();
				statement.executeUpdate(addColumnStatement);
				return true;
			} catch (Exception e) {
				return false;
			} finally {
				closeQuietly(statement);
				closeQuietly(connection);
			}
		}
	}

	public static boolean alterColumnTypeInDbTable(DataSource dataSource, String tablename, String fieldname, String fieldType, int length, String fieldDefault, boolean notNull) throws Exception {
		if (Utilities.isBlank(fieldname)) {
			return false;
		} else if (!tablename.equalsIgnoreCase(getSQLSafeString(tablename))) {
			return false;
		} else if (Utilities.isBlank(fieldname)) {
			return false;
		} else if (!fieldname.equalsIgnoreCase(getSQLSafeString(fieldname))) {
			return false;
		} else if (Utilities.isBlank(fieldType)) {
			return false;
		} else if (!containsColumnName(dataSource, tablename, fieldname)) {
			return false;
		} else {
			fieldType = fieldType.toUpperCase().trim();

			String changeColumnStatementPart = fieldname.toLowerCase() + " " + fieldType;
			if (fieldType.startsWith("VARCHAR")) {
				if (length <= 0) {
					length = 100;
				}
				changeColumnStatementPart += "(" + length + ")";
			}

			// Default Value
			if (Utilities.isNotEmpty(fieldDefault)) {
				if (fieldType.equalsIgnoreCase("VARCHAR")) {
					changeColumnStatementPart += " DEFAULT '" + fieldDefault + "'";
				} else if (fieldType.equalsIgnoreCase("DATE")) {
					changeColumnStatementPart += " DEFAULT " + getDateDefaultValue(dataSource, fieldDefault);
				} else {
					changeColumnStatementPart += " DEFAULT " + fieldDefault;
				}
			}

			// Maybe null
			if (notNull) {
				changeColumnStatementPart += " NOT NULL";
			}

			String changeColumnStatement;
			DbVendor dbVendor = getDbVendor(dataSource);
			if (DbVendor.Oracle == dbVendor) {
				changeColumnStatement = "ALTER TABLE " + tablename + " MODIFY (" + changeColumnStatementPart + ")";
			} else if (DbVendor.MySQL == dbVendor) {
				changeColumnStatement = "ALTER TABLE " + tablename + " MODIFY " + changeColumnStatementPart;
			} else {
				throw new Exception("Unsupported db vendor");
			}

			Connection connection = null;
			Statement statement = null;
			try {
				connection = dataSource.getConnection();
				statement = connection.createStatement();
				statement.executeUpdate(changeColumnStatement);
				return true;
			} catch (Exception e) {
				return false;
			} finally {
				closeQuietly(statement);
				closeQuietly(connection);
			}
		}
	}

	public static String getSQLSafeString(String value) {
		if (value == null) {
			return null;
		} else {
			return value.replace("'", "''");
		}
	}

	public static boolean checkOracleTablespaceExists(DataSource dataSource, String tablespaceName) throws Exception {
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
			return checkOracleTablespaceExists(connection, tablespaceName);
		} catch (SQLException e) {
			throw new RuntimeException("Cannot check db tablespace " + tablespaceName + ": " + e.getMessage(), e);
		} finally {
			closeQuietly(connection);
		}
	}

	/**
	 * Check if oracle tablespace exists
	 *
	 * @param connection
	 * @param tablespaceName
	 * @return
	 * @throws Exception 
	 */
	public static boolean checkOracleTablespaceExists(Connection connection, String tablespaceName) throws Exception {
		DbVendor dbVendor = getDbVendor(connection);
		if (dbVendor == DbVendor.Oracle && tablespaceName != null) {
			PreparedStatement statement = null;
			ResultSet resultSet = null;

			try {
				statement = connection.prepareStatement("SELECT COUNT(*) FROM dba_tablespaces WHERE LOWER(tablespace_name) = ?");
				statement.setString(1, tablespaceName.toLowerCase());

				resultSet = statement.executeQuery();

				return resultSet.getInt(1) > 0;
			} catch (Exception e) {
				throw new RuntimeException("Cannot check db tablespace " + tablespaceName + ": " + e.getMessage(), e);
			} finally {
				closeQuietly(resultSet);
				resultSet = null;
				closeQuietly(statement);
				statement = null;
			}
		} else {
			return false;
		}
	}

	public static String getPrimaryKeyColumn(DataSource dataSource, String tableName) {
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
			return getPrimaryKeyColumn(connection, tableName);
		} catch (SQLException e) {
			throw new RuntimeException("Cannot read primarykey column for table " + tableName + ": " + e.getMessage(), e);
		} finally {
			closeQuietly(connection);
		}
	}

	public static String getPrimaryKeyColumn(Connection connection, String tableName) {
		if (Utilities.isBlank(tableName)) {
			return null;
		} else {
			ResultSet resultSet = null;
			try {
				DatabaseMetaData metaData = connection.getMetaData();
				resultSet = metaData.getPrimaryKeys(null, null, tableName);

				if (resultSet.next()) {
					return resultSet.getString("COLUMN_NAME");
				} else {
					return null;
				}
			} catch (Exception e) {
				throw new RuntimeException("Cannot read primarykey column for table " + tableName + ": " + e.getMessage(), e);
			} finally {
				closeQuietly(resultSet);
				resultSet = null;
			}
		}
	}
	
	/**
	 * tablePatternExpression contains a comma-separated list of tablenames with wildcards *? and !(not, before tablename)
	 * 
	 * @param connection
	 * @param tablePatternExpression
	 * @return
	 * @throws Exception
	 */
	public static List<String> getAvailableTables(Connection connection, String tablePatternExpression) throws Exception {
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			DbVendor dbVendor = getDbVendor(connection);
			statement = connection.createStatement();

			String tableQuery;
			if (DbVendor.Oracle == dbVendor) {
				tableQuery = "SELECT DISTINCT table_name FROM all_tables WHERE owner NOT IN ('CTXSYS', 'DBSNMP', 'MDDATA', 'MDSYS', 'DMSYS', 'OLAPSYS', 'ORDPLUGINS', 'OUTLN', 'SI_INFORMATN_SCHEMA', 'SYS', 'SYSMAN', 'SYSTEM')";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().toUpperCase().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND table_name NOT LIKE '" + tablePattern.substring(1) + "' ESCAPE '\\'";
						} else {
							tableQuery += " AND table_name LIKE '" + tablePattern + "' ESCAPE '\\'";
						}
					}
				}
				tableQuery += " ORDER BY table_name";

				resultSet = statement.executeQuery(tableQuery);
				List<String> tableNamesToExport = new ArrayList<String>();
				while (resultSet.next()) {
					tableNamesToExport.add(resultSet.getString("table_name"));
				}
				return tableNamesToExport;
			} else if (DbVendor.MySQL == dbVendor) {
				tableQuery = "SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema')";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND table_name NOT LIKE '" + tablePattern.substring(1) + "'";
						} else {
							tableQuery += " AND table_name LIKE '" + tablePattern + "'";
						}
					}
				}
				tableQuery += " ORDER BY table_name";

				resultSet = statement.executeQuery(tableQuery);
				List<String> tableNamesToExport = new ArrayList<String>();
				while (resultSet.next()) {
					tableNamesToExport.add(resultSet.getString("table_name"));
				}
				return tableNamesToExport;
			} else if (DbVendor.PostgreSQL == dbVendor) {
				tableQuery = "SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().toUpperCase().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND table_name NOT LIKE '" + tablePattern.substring(1) + "' ESCAPE '\\'";
						} else {
							tableQuery += " AND table_name LIKE '" + tablePattern + "' ESCAPE '\\'";
						}
					}
				}
				tableQuery += " ORDER BY table_name";

				resultSet = statement.executeQuery(tableQuery);
				List<String> tableNamesToExport = new ArrayList<String>();
				while (resultSet.next()) {
					tableNamesToExport.add(resultSet.getString("table_name"));
				}
				return tableNamesToExport;
			} else if (DbVendor.SQLite == dbVendor) {
				tableQuery = "SELECT name FROM sqlite_master WHERE type = 'table'";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().toUpperCase().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND name NOT LIKE '" + tablePattern.substring(1) + "' ESCAPE '\\'";
						} else {
							tableQuery += " AND name LIKE '" + tablePattern + "' ESCAPE '\\'";
						}
					}
				}
				tableQuery += " ORDER BY name";
				
				resultSet = statement.executeQuery(tableQuery);
				List<String> tableNamesToExport = new ArrayList<String>();
				while (resultSet.next()) {
					tableNamesToExport.add(resultSet.getString("name"));
				}
				return tableNamesToExport;
			}  else if (DbVendor.Derby == dbVendor) {
				tableQuery = "SELECT tablename FROM sys.systables WHERE tabletype = 'T'";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().toUpperCase().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND tablename NOT LIKE '" + tablePattern.substring(1) + "' {ESCAPE '\\'}";
						} else {
							tableQuery += " AND tablename LIKE '" + tablePattern + "' {ESCAPE '\\'}";
						}
					}
				}
				tableQuery += " ORDER BY tablename";
				
				resultSet = statement.executeQuery(tableQuery);
				List<String> tableNamesToExport = new ArrayList<String>();
				while (resultSet.next()) {
					tableNamesToExport.add(resultSet.getString("tablename"));
				}
				return tableNamesToExport;
			} else {
				throw new Exception("Unknown db vendor");
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			if (statement != null) {
				try {
					statement.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}
