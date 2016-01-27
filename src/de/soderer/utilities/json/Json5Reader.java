package de.soderer.utilities.json;

import java.io.InputStream;
import java.util.regex.Pattern;

/**
 * Differences to standard JSON:<br />
 * Objects:<br />
 * 	Unquoted Object keys<br />
 * 	Single-quoted Object keys<br />
 *  Objects properties list may have trailing comma<br />
 * Arrays:<br />
 * 	Arrays item list may have trailing comma<br />
 * Strings:<br />
 * 	Single-quoted Strings<br />
 * 	Strings can be split across multiple lines (escape newlines with backslashes)<br />
 * Numbers:<br />
 * 	Hexadecimal numbers as unquoted values<br />
 * 	Numbers may begin or end with a (leading or trailing) decimal point<br />
 * 	Numbers include Infinity, -Infinity, NaN, and -NaN<br />
 * 	Numbers may begin with an explicit plus sign<br />
 * Comments:<br />
 * 	Inline comment (single-line up to the line end)<br />
 * 	Block comment (multi-line)<br />
 */
public class Json5Reader extends JsonReader {
	public Json5Reader(InputStream inputStream) {
		this(inputStream, null);
	}
	
	public Json5Reader(InputStream inputStream, String encoding) {
		super(inputStream, encoding);
	}
	
	@Override
	public JsonToken readNextToken() throws Exception {
		currentObject = null;
		switch (readNextNonWhitespace()) {
			case '{':
				if (openJsonItems.size() > 0 && openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					openJsonItems.pop();
				}
				openJsonItems.push(JsonToken.JsonObject_Open);
				return JsonToken.JsonObject_Open;
			case '}':
				if (openJsonItems.pop() != JsonToken.JsonObject_Open) {
					throw new Exception("Invalid json data at index " + getReadCharacters());
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
					throw new Exception("Invalid json data at index " + getReadCharacters());
				} else {
					return JsonToken.JsonArray_Close;
				}
			case '"':
				if (openJsonItems.peek() == JsonToken.JsonArray_Open) {
					currentObject = readQuotedText('"', '\\');
					return JsonToken.JsonSimpleValue;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_Open) {
					currentObject = readQuotedText('"', '\\');
					char currentChar = readNextNonWhitespace();
					if (currentChar != ':') {
						throw new Exception("Invalid json data at index " + getReadCharacters());
					}
					openJsonItems.push(JsonToken.JsonObject_PropertyKey);
					return JsonToken.JsonObject_PropertyKey;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					currentObject = readQuotedText('"', '\\');
					openJsonItems.pop();
					char currentChar = readNextNonWhitespace();
					if (currentChar == '}') {
						reuseCurrentChar();
					} else if (currentChar != ',') {
						throw new Exception("Invalid json data at index " + getReadCharacters());
					}
					return JsonToken.JsonSimpleValue;
				} else {
					throw new Exception("Invalid json data at index " + getReadCharacters());
				}
			case '\'':
				if (openJsonItems.peek() == JsonToken.JsonArray_Open) {
					currentObject = readQuotedText('\'', '\\');
					return JsonToken.JsonSimpleValue;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_Open) {
					currentObject = readQuotedText('\'', '\\');
					char currentChar = readNextNonWhitespace();
					if (currentChar != ':') {
						throw new Exception("Invalid json data at index " + getReadCharacters());
					}
					openJsonItems.push(JsonToken.JsonObject_PropertyKey);
					return JsonToken.JsonObject_PropertyKey;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					currentObject = readQuotedText('\'', '\\');
					openJsonItems.pop();
					char currentChar = readNextNonWhitespace();
					if (currentChar == '}') {
						reuseCurrentChar();
					} else if (currentChar != ',') {
						throw new Exception("Invalid json data at index " + getReadCharacters());
					}
					return JsonToken.JsonSimpleValue;
				} else {
					throw new Exception("Invalid json data at index " + getReadCharacters());
				}
			case ',':
				return readNextToken();
			case '/':
				char currentChar = readNextCharacter();
				if (currentChar == '/') {
					readUpToNext(false, null, '\n');
				} else if (currentChar == '*') {
					while ((currentChar = readNextCharacter()) != '/') {
						readUpToNext(true, null, '*');
					}
				} else {
					throw new Exception("Invalid json data at index " + getReadCharacters());
				}
				return readNextToken();
			default:
				if (openJsonItems.peek() == JsonToken.JsonObject_Open) {
					currentObject = readJsonIdentifier(readUpToNext(false, null, ':').trim());
					readNextCharacter();
					openJsonItems.push(JsonToken.JsonObject_PropertyKey);
					return JsonToken.JsonObject_PropertyKey;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					openJsonItems.pop();
					currentObject = readSimpleJsonValue(readUpToNext(false, null, ',', '}').trim());
					char nextCharAfterSimpleValue = readNextNonWhitespace();
					if (nextCharAfterSimpleValue == '}') {
						reuseCurrentChar();
					}
					return JsonToken.JsonSimpleValue;
				} else if (openJsonItems.peek() == JsonToken.JsonArray_Open) {
					currentObject = readSimpleJsonValue(readUpToNext(false, null, ',', ']').trim());
					char nextCharAfterSimpleValue = readNextNonWhitespace();
					if (nextCharAfterSimpleValue == ']') {
						reuseCurrentChar();
					}
					return JsonToken.JsonSimpleValue;
				} else {
					throw new Exception("Invalid json data at index " + getReadCharacters());
				}
		}
	}

	private Object readSimpleJsonValue(String valueString) throws Exception {
		if (valueString.equalsIgnoreCase("null")) {
			return null;
		} else if (valueString.equalsIgnoreCase("true")) {
			return true;
		} else if (valueString.equalsIgnoreCase("false")) {
			return false;
		} else if (valueString.equalsIgnoreCase("Infinity")) {
			return Double.POSITIVE_INFINITY;
		} else if (valueString.equalsIgnoreCase("-Infinity")) {
			return Double.NEGATIVE_INFINITY;
		} else if (valueString.equalsIgnoreCase("NaN")) {
			return Double.NaN;
		} else if (valueString.equalsIgnoreCase("-NaN")) {
			return Double.NaN;
		} else if (Pattern.matches("0(x|X)[0-9A-Fa-f]+", valueString)) {
			Long value = Long.parseLong(valueString.substring(2), 16);
			if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE ) {
				return new Integer(valueString);
			} else {
				return value;
			}
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
			throw new Exception("Invalid json data at index " + getReadCharacters());
		}
	}

	/**
	 * A valid identifier must
	 * 	start with a letter (A-Za-z), underscore (_) or dollar sign ($)
	 * 	subsequent characters can also be digits (0-9)
	 * 
	 * @param identifierString
	 * @return
	 * @throws Exception
	 */
	private String readJsonIdentifier(String identifierString) throws Exception {
		if (Pattern.matches("[A-Za-z_$]+[A-Za-z_$0-9]*", identifierString)) {
			return identifierString;
		} else {
			throw new Exception("Invalid json identifier at index " + getReadCharacters());
		}
	}
}
