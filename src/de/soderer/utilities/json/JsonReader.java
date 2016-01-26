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
			currentChar = readNextNonWhitespace();
			
			if (currentChar == ',') {
				if (openJsonItems.peek() != JsonToken.JsonObject_Open && openJsonItems.peek() != JsonToken.JsonArray_Open) {
					throw new Exception("Invalid json data at index " + readCharacters);
				} else {
					currentChar = readNextNonWhitespace();
				}
			} else if (currentChar == ':') {
				if (openJsonItems.peek() != JsonToken.JsonObject_PropertyKey) {
					throw new Exception("Invalid json data at index " + readCharacters);
				} else {
					currentChar = readNextNonWhitespace();
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
				currentObject = readQuotedText();
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
					String rawValueString = readUpToNext(',', '}');
					currentObject = readSimpleJsonValue(currentChar + rawValueString.substring(0, rawValueString.length() - 1).trim());
					if (rawValueString.charAt(rawValueString.length() - 1) == '}') {
						reuseChar = '}';
					}
					openJsonItems.pop();
					return JsonToken.JsonSimpleValue;
				} else {
					String rawValueString = readUpToNext(',', ']');
					currentObject = readSimpleJsonValue(currentChar + rawValueString.substring(0, rawValueString.length() - 1).trim());
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
		
		JsonToken nextToken = readNextToken();
		if (nextToken == JsonToken.JsonObject_Open) {
			currentObject = readJsonObject();
			return true;
		} else if (nextToken == JsonToken.JsonArray_Open) {
			currentObject = readJsonArray();
			return true;
		} else if (nextToken == JsonToken.JsonSimpleValue) {
			// value was already read
			return true;
		} else if (nextToken == JsonToken.JsonObject_Close) {
			reuseChar = currentChar;
			openJsonItems.push(JsonToken.JsonObject_Open);
			return false;
		} else if (nextToken == JsonToken.JsonArray_Close) {
			reuseChar = currentChar;
			openJsonItems.push(JsonToken.JsonArray_Open);
			return false;
		} else {
			throw new Exception("Invalid data at index " + readCharacters);
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
		
		JsonToken nextToken = readNextToken();
		if (nextToken == JsonToken.JsonObject_Open) {
			return readJsonObject();
		} else if (nextToken == JsonToken.JsonArray_Open) {
			return readJsonArray();
		} else {
			throw new Exception("Invalid json data: No JsonObject or JsonArray at root");
		}
	}
	
	private JsonObject readJsonObject() throws Exception {
		if (currentChar != '{') {
			throw new Exception("Invalid read position for JsonArray at index " + readCharacters);
		} else {
			JsonObject returnObject = new JsonObject();
			JsonToken nextToken = readNextToken();
			while (nextToken != JsonToken.JsonObject_Close) {
				if (nextToken == JsonToken.JsonObject_PropertyKey && currentObject instanceof String) {
					String propertyKey = (String) currentObject;
					nextToken = readNextToken();
					if (nextToken == JsonToken.JsonArray_Open) {
						returnObject.add(propertyKey, readJsonArray());
					} else if (nextToken == JsonToken.JsonObject_Open) {
						returnObject.add(propertyKey, readJsonObject());
					} else if (nextToken == JsonToken.JsonSimpleValue) {
						returnObject.add(propertyKey, currentObject);
					} else {
						throw new Exception("Unexpected JsonToken " + nextToken + " at index " + readCharacters);
					}
					nextToken = readNextToken();
				} else {
					throw new Exception("Unexpected JsonToken " + nextToken + " at index " + readCharacters);
				}
			}
			return returnObject;
		}
	}
	
	private JsonArray readJsonArray() throws Exception {
		if (currentChar != '[') {
			throw new Exception("Invalid read position for JsonArray at index " + readCharacters);
		} else {
			JsonToken nextToken = readNextToken();
			if (nextToken == JsonToken.JsonArray_Close
					|| nextToken == JsonToken.JsonObject_Open
					|| nextToken == JsonToken.JsonArray_Open
					|| nextToken == JsonToken.JsonSimpleValue) {
				JsonArray returnArray = new JsonArray();
				while (nextToken != JsonToken.JsonArray_Close) {
					if (nextToken == JsonToken.JsonArray_Open) {
						returnArray.add(readJsonArray());
					} else if (nextToken == JsonToken.JsonObject_Open) {
						returnArray.add(readJsonObject());
					} else if (nextToken == JsonToken.JsonSimpleValue) {
						returnArray.add(currentObject);
					}
					nextToken = readNextToken();
				}
				return returnArray;
			} else {
				throw new Exception("Unexpected JsonToken " + nextToken + " at index " + readCharacters);
			}
		}
	}

	private char readNextNonWhitespace() throws Exception {
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

	private String readQuotedText() throws Exception {
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

	private String readUpToNext(char... endChars) throws Exception {
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
