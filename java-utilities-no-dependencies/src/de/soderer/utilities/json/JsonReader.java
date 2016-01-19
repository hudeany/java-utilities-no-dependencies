package de.soderer.utilities.json;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Stack;
import java.util.regex.Pattern;

public class JsonReader implements Closeable {
	// TODO: 65535?
	
	/** Default input encoding. */
	public static final String DEFAULT_ENCODING = "UTF-8";
	
	/** Input stream. */
	private InputStream inputStream;

	/** Input encoding. */
	private Charset encoding;

	/** Input reader. */
	private BufferedReader inputReader = null;
	
	private char currentChar;
	private Character reuseChar = null;
	private Object currentObject = null;
	private long readCharacters = 0;
	
	private Stack<JsonToken> openJsonItems = new Stack<JsonToken>();
	
	public enum JsonToken {
		JsonObject_Open,
		JsonObject_PropertyKey,
		JsonObject_Close,
		JsonArray_Open,
		JsonArray_Close,
		JsonSimpleValue,
	}

	public JsonReader(InputStream inputStream) {
		this(inputStream, null);
	}
	
	public JsonReader(InputStream inputStream, String encoding) {
		this.inputStream = inputStream;
		this.encoding = isBlank(encoding) ? Charset.forName(DEFAULT_ENCODING) : Charset.forName(encoding);
	}
	
	public Object getCurrentObject() {
		return currentObject;
	}
	
	public JsonToken readNextToken() throws Exception {
		if (inputReader == null) {
			inputReader = new BufferedReader(new InputStreamReader(inputStream, encoding));
		}
		
		if (reuseChar != null) {
			currentChar = reuseChar;
			reuseChar = null;
		} else {
			currentChar = readNextNonWhitespace(inputReader);
			
			if (currentChar == ',') {
				if (openJsonItems.peek() != JsonToken.JsonObject_Open && openJsonItems.peek() != JsonToken.JsonArray_Open) {
					throw new Exception("Invalid json data at index " + readCharacters);
				} else {
					currentChar = readNextNonWhitespace(inputReader);
				}
			} else if (currentChar == ':') {
				if (openJsonItems.peek() != JsonToken.JsonObject_PropertyKey) {
					throw new Exception("Invalid json data at index " + readCharacters);
				} else {
					currentChar = readNextNonWhitespace(inputReader);
				}
			}
		}
		
		currentObject = null;
		switch (currentChar) {
			case '{':
				if (openJsonItems.size() > 0 && openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					openJsonItems.pop();
				}
				openJsonItems.push(JsonToken.JsonObject_Open);
				return JsonToken.JsonObject_Open;
			case '}':
				if (openJsonItems.pop() != JsonToken.JsonObject_Open) {
					throw new Exception("Invalid json data at index " + readCharacters);
				} else {
					return JsonToken.JsonObject_Close;
				}
			case '[':
				if (openJsonItems.size() > 0 && openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					openJsonItems.pop();
				}
				openJsonItems.push(JsonToken.JsonArray_Open);
				return JsonToken.JsonArray_Open;
			case ']':
				if (openJsonItems.pop() != JsonToken.JsonArray_Open) {
					throw new Exception("Invalid json data at index " + readCharacters);
				} else {
					return JsonToken.JsonArray_Close;
				}
			case '"':
				currentObject = readQuotedText(inputReader);
				if (openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					openJsonItems.pop();
					return JsonToken.JsonSimpleValue;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_Open) {
					openJsonItems.push(JsonToken.JsonObject_PropertyKey);
					return JsonToken.JsonObject_PropertyKey;
				} else {
					return JsonToken.JsonSimpleValue;
				}
			default:
				if (openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					String rawValueString = readUpToNext(inputReader, ',', '}').trim();
					currentObject = readSimpleJsonValue(currentChar + rawValueString.substring(0, rawValueString.length() - 1));
					if (rawValueString.charAt(rawValueString.length() - 1) == '}') {
						reuseChar = '}';
					}
					openJsonItems.pop();
					return JsonToken.JsonSimpleValue;
				} else {
					String rawValueString = readUpToNext(inputReader, ',', ']').trim();
					currentObject = readSimpleJsonValue(currentChar + rawValueString.substring(0, rawValueString.length() - 1));
					if (rawValueString.charAt(rawValueString.length() - 1) == ']') {
						reuseChar = ']';
					}
					return JsonToken.JsonSimpleValue;
				}
		}
	}
	
	public boolean readNextJsonItem() throws Exception {
		if (inputReader == null) {
			throw new Exception("JsonReader position was not initialized for readNextJsonItem()");
		}
		
		if (reuseChar != null) {
			currentChar = reuseChar;
			reuseChar = null;
		} else {
			currentChar = readNextNonWhitespace(inputReader);
			
			if (currentChar == ',') {
				if (openJsonItems.peek() != JsonToken.JsonObject_Open && openJsonItems.peek() != JsonToken.JsonArray_Open) {
					throw new Exception("Invalid json data at index " + readCharacters);
				} else {
					currentChar = readNextNonWhitespace(inputReader);
				}
			} else if (currentChar == ':') {
				if (openJsonItems.peek() != JsonToken.JsonObject_PropertyKey) {
					throw new Exception("Invalid json data at index " + readCharacters);
				} else {
					currentChar = readNextNonWhitespace(inputReader);
				}
			}
		}
		
		currentObject = null;
		switch (currentChar) {
			case '}':
			case ']':
				reuseChar = currentChar;
				return false;
			case '{':
				currentObject = readJsonObject(inputReader);
				return true;
			case '[':
				currentObject = readJsonArray(inputReader);
				return true;
			case '"':
				currentObject = readQuotedText(inputReader);
				return true;
			default:
				if (openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					String rawValueString = readUpToNext(inputReader, ',', '}');
					currentObject = readSimpleJsonValue(currentChar + rawValueString.substring(0, rawValueString.length() - 1).trim());
					if (rawValueString.charAt(rawValueString.length() - 1) == '}') {
						reuseChar = '}';
					}
				} else {
					String rawValueString = readUpToNext(inputReader, ',', ']');
					currentObject = readSimpleJsonValue(currentChar + rawValueString.substring(0, rawValueString.length() - 1).trim());
					if (rawValueString.charAt(rawValueString.length() - 1) == ']') {
						reuseChar = ']';
					}
				}
				return true;
		}
	}
	
	/**
	 * Read all available Json data from the input stream at once.
	 * This can only be done once and as the first action on a JsonReader.
	 * 
	 * @return JsonObject or JsonArray
	 * @throws Exception
	 */
	public JsonItem read() throws Exception {
		if (inputReader == null) {
			inputReader = new BufferedReader(new InputStreamReader(inputStream, encoding));
		} else {
			throw new Exception("JsonReader position was already initialized for other read operation");
		}
		
		currentChar = readNextNonWhitespace(inputReader);
		if (currentChar == '{') {
			return readJsonObject(inputReader);
		} else if (currentChar == '[') {
			return readJsonArray(inputReader);
		} else {
			throw new Exception("Invalid json data: No Json object or Json array at root");
		}
	}
	
	private JsonObject readJsonObject(BufferedReader inputReader) throws Exception {
		reuseChar = null;
		JsonObject jsonObject = new JsonObject();
		currentChar = readNextNonWhitespace(inputReader);
		if (currentChar == '}') {
			// Empty Json object
			return jsonObject;
		} else {
			while (true) {
				if (currentChar == '"') {
					// Read Json object property key
					String key = readQuotedText(inputReader);
					currentChar = readNextNonWhitespace(inputReader);
					if (currentChar != ':') {
						throw new Exception("Invalid json data at index " + readCharacters);
					}
					
					// Read Json object property value
					currentChar = readNextNonWhitespace(inputReader);
					Object value;
					if (currentChar == '{') {
						value = readJsonObject(inputReader);
						currentChar = readNextNonWhitespace(inputReader);
					} else if (currentChar == '[') {
						value = readJsonArray(inputReader);
						currentChar = readNextNonWhitespace(inputReader);
					} else if (currentChar == '"') {
						value = readQuotedText(inputReader);
						currentChar = readNextNonWhitespace(inputReader);
					} else {
						String rawValueString = readUpToNext(inputReader, ',', '}');
						value = readSimpleJsonValue(currentChar + rawValueString.substring(0, rawValueString.length() - 1).trim());
						currentChar = rawValueString.charAt(rawValueString.length() - 1);
						if (currentChar == '}') {
							reuseChar = currentChar;
						}
					}
					
					jsonObject.add(key, value);
					
					if (currentChar == '}') {
						return jsonObject;
					} else if (currentChar == ',') {
						currentChar = readNextNonWhitespace(inputReader);
					} else {
						throw new Exception("Invalid json data at index " + readCharacters);
					}
				} else {
					throw new Exception("Invalid json data at index " + readCharacters);
				}
			}
		}
	}
	
	private JsonArray readJsonArray(BufferedReader inputReader) throws Exception {
		reuseChar = null;
		JsonArray jsonArray = new JsonArray();
		currentChar = readNextNonWhitespace(inputReader);
		if (currentChar == ']') {
			// Empty Json array
			return jsonArray;
		} else {
			while (true) {
				// Read Json array value
				Object value;
				if (currentChar == '{') {
					value = readJsonObject(inputReader);
					currentChar = readNextNonWhitespace(inputReader);
				} else if (currentChar == '[') {
					value = readJsonArray(inputReader);
					currentChar = readNextNonWhitespace(inputReader);
				} else if (currentChar == '"') {
					value = readQuotedText(inputReader);
					currentChar = readNextNonWhitespace(inputReader);
				} else {
					String rawValueString = readUpToNext(inputReader, ',', ']');
					value = readSimpleJsonValue(currentChar + rawValueString.substring(0, rawValueString.length() - 1).trim());
					currentChar = rawValueString.charAt(rawValueString.length() - 1);
					if (currentChar == ']') {
						reuseChar = currentChar;
					}
				}
				
				jsonArray.add(value);
				
				if (currentChar == ']') {
					return jsonArray;
				} else if (currentChar == ',') {
					currentChar = readNextNonWhitespace(inputReader);
				} else {
					throw new Exception("Invalid json data at index " + readCharacters);
				}
			}
		}
	}

	private char readNextNonWhitespace(BufferedReader inputReader) throws Exception {
		int currentCharInt;
		while ((currentCharInt = inputReader.read()) != -1) {
			currentChar = (char) currentCharInt;
			readCharacters++;
			if (!Character.isWhitespace(currentChar)) {
				return currentChar;
			}
		}
		throw new Exception("Invalid json data: premature end of data");
	}

	private String readQuotedText(BufferedReader inputReader) throws Exception {
		StringBuilder returnValue = new StringBuilder();
		boolean escapeNextChar = false;
		int currentCharInt;
		while ((currentCharInt = (char) inputReader.read()) != -1) {
			char nextChar = (char) currentCharInt;
			readCharacters++;
			if (nextChar == '"' && !escapeNextChar) {
				return returnValue.toString();
			} else if (nextChar == '\\' && !escapeNextChar) {
				escapeNextChar = true;
			} else {
				returnValue.append(nextChar);
				escapeNextChar = false;
			}
		}
		throw new Exception("Invalid json data: premature end of data");
	}

	private String readUpToNext(BufferedReader inputReader, char... endChars) throws Exception {
		StringBuilder returnValue = new StringBuilder();
		int currentCharInt;
		while ((currentCharInt = (char) inputReader.read()) != -1) {
			char nextChar = (char) currentCharInt;
			readCharacters++;
			returnValue.append(nextChar);
			for (char endChar : endChars) {
				if (endChar == nextChar) {
					return returnValue.toString();
				}
			}
		}
		throw new Exception("Invalid json data: premature end of data");
	}

	private Object readSimpleJsonValue(String valueString) throws Exception {
		if (valueString.equalsIgnoreCase("null")) {
			return null;
		} else if (valueString.equalsIgnoreCase("true")) {
			return true;
		} else if (valueString.equalsIgnoreCase("false")) {
			return false;
		} else if (Pattern.matches("[+|-]?[0-9]*(\\.[0-9]*)?([e|E][+|-]?[0-9]*)?", valueString)) {
			if (valueString.contains(".")) {
				return new Double(valueString);
			} else {
				Long value = new Long(valueString);
				if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE ) {
					return new Integer(valueString);
				} else {
					return value;
				}
			}
		} else {
			throw new Exception("Invalid json data at index " + readCharacters);
		}
	}

	/**
	 * Close this writer and its underlying stream.
	 */
	@Override
	public void close() throws IOException {
		closeQuietly(inputReader);
		inputReader = null;
		closeQuietly(inputStream);
		inputStream = null;
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
	
	public class JsonNullValue implements JsonItem {
		@Override
		public boolean isJsonObject() {
			return false;
		}

		@Override
		public boolean isJsonArray() {
			return false;
		}

		@Override
		public String toString() {
			return "null";
		}
	}
}
