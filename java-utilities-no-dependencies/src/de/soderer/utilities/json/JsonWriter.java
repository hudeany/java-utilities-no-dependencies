package de.soderer.utilities.json;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Map.Entry;
import java.util.Stack;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.Utilities;

public class JsonWriter implements Closeable {
	/** Default output encoding. */
	public static final String DEFAULT_ENCODING = "UTF-8";
	
	/** Output stream. */
	private OutputStream outputStream;

	/** Output encoding. */
	private Charset encoding;

	/** Output writer. */
	private BufferedWriter outputWriter = null;
	
	private Stack<JsonStackItem> openJsonStackItems = new Stack<JsonStackItem>();
	
	private String linebreak = "\n";
	private String indention = "\t";
	private String separator = " ";

	private enum JsonStackItem {
		Array_Empty,
		Array,
		Object_Empty,
		Object,
		Object_Value
	}
	
	public JsonWriter(OutputStream outputStream) {
		this(outputStream, null);
	}
	
	public JsonWriter(OutputStream outputStream, String encoding) {
		this.outputStream = outputStream;
		this.encoding = isBlank(encoding) ? Charset.forName(DEFAULT_ENCODING) : Charset.forName(encoding);
	}
	
	public void setIndentation(String indentation) {
		this.indention = indentation;
	}
	
	public void setIndentation(char indentationCharacter) {
		this.indention = Character.toString(indentationCharacter);
	}
	
	public void setUglify(boolean value) {
		if (value) {
			linebreak = "";
			indention = "";
			separator = "";
		} else {
			linebreak = "\n";
			indention = "\t";
			separator = " ";
		}
	}
	
	public void openJsonObject() throws Exception {
		if (outputWriter == null) {
			write("{", true);
			openJsonStackItems.push(JsonStackItem.Object_Empty);
		} else {
			JsonStackItem latestOpenJsonItem = openJsonStackItems.pop();
			if (latestOpenJsonItem != JsonStackItem.Array_Empty && latestOpenJsonItem != JsonStackItem.Array && latestOpenJsonItem != JsonStackItem.Object_Value) {
				openJsonStackItems.push(latestOpenJsonItem);
				throw new Exception("Not matching open Json item for opening object: " + latestOpenJsonItem);
			} else {
				if (latestOpenJsonItem == JsonStackItem.Array) {
					write("," + linebreak, false);
				} else if (latestOpenJsonItem == JsonStackItem.Array_Empty) {
					write(linebreak, false);
				} else if (latestOpenJsonItem == JsonStackItem.Object_Value) {
					openJsonStackItems.push(JsonStackItem.Object_Value);
					write(linebreak, false);
				}
				
				if (latestOpenJsonItem != JsonStackItem.Object_Value) {
					openJsonStackItems.push(JsonStackItem.Array);
				}
				
				write("{", true);
				openJsonStackItems.push(JsonStackItem.Object_Empty);
			}
		}
	}
	
	public void openJsonObjectProperty(String propertyName) throws Exception {
		JsonStackItem latestOpenJsonItem = openJsonStackItems.pop();
		if (latestOpenJsonItem != JsonStackItem.Object_Empty && latestOpenJsonItem != JsonStackItem.Object) {
			openJsonStackItems.push(latestOpenJsonItem);
			throw new Exception("Not matching open Json item for opening object property: " + latestOpenJsonItem);
		} else {
			if (latestOpenJsonItem == JsonStackItem.Object) {
				write("," + linebreak, false);
			} else {
				write(linebreak, false);
			}
			openJsonStackItems.push(JsonStackItem.Object);
			write("\"" + propertyName.replace("\"", "\\\"") + "\":", true);
			openJsonStackItems.push(JsonStackItem.Object_Value);
		}
	}
	
	public void addSimpleJsonObjectPropertyValue(Object propertyValue) throws Exception {
		JsonStackItem latestOpenJsonItem = openJsonStackItems.pop();
		if (latestOpenJsonItem != JsonStackItem.Object_Value) {
			openJsonStackItems.push(latestOpenJsonItem);
			throw new Exception("Not matching open Json item for adding object property value: " + latestOpenJsonItem);
		} else {
			if (propertyValue == null) {
				write(separator + "null", false);
			} else if (propertyValue instanceof Boolean) {
				write(separator + Boolean.toString((Boolean) propertyValue), false);
			} else if (propertyValue instanceof Date) {
				write(separator + "\"" + DateUtilities.ISO_8601_DATETIME_FORMAT.format((Date) propertyValue) + "\"", false);
			} else if (propertyValue instanceof Number) {
				write(separator + ((Number) propertyValue).toString(), false);
			} else {
				write(separator + "\"" + propertyValue.toString().replace("\"", "\\\"") + "\"", false);
			}
		}
	}
	
	public void closeJsonObject() throws Exception {
		JsonStackItem latestOpenJsonItem = openJsonStackItems.pop();
		if (latestOpenJsonItem != JsonStackItem.Object_Empty && latestOpenJsonItem != JsonStackItem.Object) {
			openJsonStackItems.push(latestOpenJsonItem);
			throw new Exception("Not matching open Json item for closing object: " + latestOpenJsonItem);
		} else if (latestOpenJsonItem == JsonStackItem.Object_Empty) {
			write("}", false);
		} else {
			write(linebreak, false);
			write("}", true);
		}
		
		if (openJsonStackItems.size() > 0 && openJsonStackItems.peek() == JsonStackItem.Object_Value) {
			openJsonStackItems.pop();
		}
	}
	
	public void openJsonArray() throws Exception {
		if (outputWriter == null) {
			write("[", true);
			openJsonStackItems.push(JsonStackItem.Array_Empty);
		} else {
			JsonStackItem latestOpenJsonItem = openJsonStackItems.pop();
			if (latestOpenJsonItem != JsonStackItem.Array_Empty && latestOpenJsonItem != JsonStackItem.Array && latestOpenJsonItem != JsonStackItem.Object_Value) {
				openJsonStackItems.push(latestOpenJsonItem);
				throw new Exception("Not matching open Json item for opening array: " + latestOpenJsonItem);
			} else {
				if (latestOpenJsonItem == JsonStackItem.Array) {
					write("," + linebreak, false);
				} else if (latestOpenJsonItem == JsonStackItem.Array_Empty) {
					write(linebreak, false);
				} else if (latestOpenJsonItem == JsonStackItem.Object_Value) {
					openJsonStackItems.push(JsonStackItem.Object_Value);
					write(linebreak, false);
				}
				
				if (latestOpenJsonItem != JsonStackItem.Object_Value) {
					openJsonStackItems.push(JsonStackItem.Array);
				}
				
				write("[", true);
				openJsonStackItems.push(JsonStackItem.Array_Empty);
			}
		}
	}
	
	public void addSimpleJsonArrayValue(Object arrayValue) throws Exception {
		JsonStackItem latestOpenJsonItem = openJsonStackItems.pop();
		if (latestOpenJsonItem != JsonStackItem.Array_Empty && latestOpenJsonItem != JsonStackItem.Array) {
			openJsonStackItems.push(latestOpenJsonItem);
			throw new Exception("Not matching open Json item for adding array value: " + latestOpenJsonItem);
		} else {
			if (latestOpenJsonItem == JsonStackItem.Array) {
				write("," + linebreak, false);
			} else {
				write(linebreak, false);
			}

			openJsonStackItems.push(JsonStackItem.Array);
			
			if (arrayValue == null) {
				write("null", true);
			} else if (arrayValue instanceof Boolean) {
				write(Boolean.toString((Boolean) arrayValue), true);
			} else if (arrayValue instanceof Date) {
				write("\"" + DateUtilities.ISO_8601_DATETIME_FORMAT.format((Date) arrayValue) + "\"", true);
			} else if (arrayValue instanceof Number) {
				write(((Number) arrayValue).toString(), true);
			} else {
				write("\"" + arrayValue.toString().replace("\"", "\\\"") + "\"", true);
			}
		}
	}
	
	public void closeJsonArray() throws Exception {
		JsonStackItem latestOpenJsonItem = openJsonStackItems.pop();
		if (latestOpenJsonItem != JsonStackItem.Array_Empty && latestOpenJsonItem != JsonStackItem.Array) {
			openJsonStackItems.push(latestOpenJsonItem);
			throw new Exception("Not matching open Json item for closing array: " + latestOpenJsonItem);
		} else if (latestOpenJsonItem == JsonStackItem.Array_Empty) {
			write("]", false);
		} else {
			write(linebreak, false);
			write("]", true);
		}
		
		if (openJsonStackItems.size() > 0 && openJsonStackItems.peek() == JsonStackItem.Object_Value) {
			openJsonStackItems.pop();
		}
	}
	
	public void add(JsonItem jsonItem) throws Exception {
		if (jsonItem == null) {
			throw new Exception("Invalid null value added via 'add'. If done by intention use 'addSimpleJsonArrayValue' or 'addSimpleJsonObjectPropertyValue'");
		} else if (jsonItem.isJsonObject()) {
			JsonObject jsonObject = (JsonObject) jsonItem;
			openJsonObject();
			for (Entry<String, Object> property : jsonObject) {
				openJsonObjectProperty(property.getKey());
				Object propertyValue = property.getValue();
				if (propertyValue instanceof JsonObject) {
					add((JsonObject) propertyValue);
				} else if (propertyValue instanceof JsonArray) {
					add((JsonArray) propertyValue);
				} else {
					addSimpleJsonObjectPropertyValue(propertyValue);
				}
			}
			closeJsonObject();
		} else {
			JsonArray jsonArray = (JsonArray) jsonItem;
			openJsonArray();
			for (Object arrayValue : jsonArray) {
				if (arrayValue instanceof JsonObject) {
					add((JsonObject) arrayValue);
				} else if (arrayValue instanceof JsonArray) {
					add((JsonArray) arrayValue);
				} else {
					addSimpleJsonArrayValue(arrayValue);
				}
			}
			closeJsonArray();
		}
	}
	
	public void closeAllOpenJsonItems() throws Exception {
		while (!openJsonStackItems.isEmpty()) {
			JsonStackItem openJsonItem = openJsonStackItems.pop();
			switch(openJsonItem) {
				case Array:
				case Array_Empty:
					closeJsonArray();
					break;
				case Object:
				case Object_Empty:
					closeJsonObject();
					break;
				case Object_Value:
					break;
			}
		}
	}

	/**
	 * Flush buffered data.
	 *
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void flush() throws IOException {
		if (outputWriter != null) {
			outputWriter.flush();
		}
	}

	/**
	 * Close this writer and its underlying stream.
	 */
	@Override
	public void close() throws IOException {
		if (outputWriter != null) {
			outputWriter.write(linebreak);
		}
		
		closeQuietly(outputWriter);
		outputWriter = null;
		closeQuietly(outputStream);
		outputStream = null;
		
		if (!openJsonStackItems.isEmpty()) {
			String jsonItemsStackString = "";
			while (!openJsonStackItems.isEmpty()) {
				jsonItemsStackString += "/" + openJsonStackItems.pop().toString();
			}
			throw new IOException("There are still Json items open: " + jsonItemsStackString);
		}
	}
	
	private void write(String text, boolean indent) throws IOException {
		if (outputWriter == null) {
			if (outputStream == null) {
				throw new IllegalStateException("JsonWriter is already closed");
			}
			outputWriter = new BufferedWriter(new OutputStreamWriter(outputStream, encoding));
		}
		
		outputWriter.write((indent ? Utilities.repeat(indention, openJsonStackItems.size()) : "") + text);
	}

	/**
	 * Check if String value is null or contains only whitespace characters.
	 *
	 * @param value
	 *            the value
	 * @return true, if is blank
	 */
	private static boolean isBlank(String value) {
		return value == null || value.trim().length() == 0;
	}

	/**
	 * Close a Closable item and ignore any Exception thrown by its close method.
	 *
	 * @param closeableItem
	 *            the closeable item
	 */
	private static void closeQuietly(Closeable closeableItem) {
		if (closeableItem != null) {
			try {
				closeableItem.close();
			} catch (IOException e) {
				// Do nothing
			}
		}
	}
}
