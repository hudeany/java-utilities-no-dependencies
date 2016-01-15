package de.soderer.utilities.json;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import de.soderer.utilities.TextUtilities;

public class JsonArray implements Iterable<Object> {
	private List<Object> items = new ArrayList<Object>();

	public void add(JsonObject object) {
		add((Object) object);
	}

	public void add(JsonArray array) {
		add((Object) array);
	}

	public void add(String value) {
		add((Object) value);
	}

	public void add(Number value) {
		add((Object) value);
	}

	public void add(Boolean value) {
		add((Object) value);
	}

	public void addNullValue() {
		add((Object) null);
	}

	private void add(Object value) {
		items.add(value);
	}

	public Object remove(Object value) {
		return items.remove(value);
	}

	public Object get(int index) {
		return items.get(index);
	}

	public int size() {
		return items.size();
	}

	@Override
	public Iterator<Object> iterator() {
		return items.iterator();
	}

	@Override
	public String toString() {
		return toString("\t", " ", "\n");
	}

	public String toString(String indention, String separator, String linebreak) {
		if (items.size() > 0) {
			boolean isFirstItem = true;
			StringBuilder result = new StringBuilder("[");
			result.append(separator);

			for (Object subItem : items) {
				if (!isFirstItem) {
					result.append(",");
					result.append(separator);
				}

				if (subItem == null) {
					result.append("null");
				} else if (subItem instanceof JsonObject) {
					result.append(TextUtilities.addLeadingTab(((JsonObject) subItem).toString(indention, separator, linebreak)).trim());
				} else if (subItem instanceof JsonArray) {
					result.append(TextUtilities.addLeadingTab(((JsonArray) subItem).toString(indention, separator, linebreak)).trim());
				} else if (subItem instanceof String) {
					result.append("\"");
					result.append(((String) subItem).replace("\"", "\\\""));
					result.append("\"");
				} else {
					result.append(subItem.toString());
				}

				isFirstItem = false;
			}

			result.append(separator);
			result.append("]");

			return result.toString();
		} else {
			return "[]";
		}
	}

	protected int parse(char[] jsonData, int startIndex) throws Exception {
		try {
			items.clear();

			char previousChar = ' ';
			StringBuilder quotedValue = null;
			StringBuilder unquotedValue = null;
			Object valueObject = null;

			for (int index = startIndex; index < jsonData.length; index++) {
				char charItem = jsonData[index];

				if (charItem == '\\') {
					// keep for following char check
					if (quotedValue == null) {
						throw new Exception("Unexpected '" + charItem + "'-sign at index " + index);
					}
				} else if (quotedValue != null) {
					if (charItem == '"' && previousChar != '\\') {
						valueObject = quotedValue.toString();
						quotedValue = null;
					} else if (previousChar == '\\') {
						quotedValue.append('\\');
						quotedValue.append(charItem);
					} else {
						quotedValue.append(charItem);
					}
				} else if (Character.isWhitespace(charItem)) {
					// whitespaces outside of keys and values are ignored
				} else if (charItem == ':') {
					// start value for key
					throw new Exception("Unexpected ':'-sign at index " + index);
				} else if (charItem == ',' || charItem == ']') {
					// end of value for key or array item
					if (valueObject != null || unquotedValue != null) {
						if (unquotedValue != null) {
							valueObject = JsonUtilities.getJsonValue(unquotedValue.toString());
							unquotedValue = null;
						}

						if (valueObject == null) {
							addNullValue();
						} else if (valueObject instanceof JsonObject) {
							add((JsonObject) valueObject);
						} else if (valueObject instanceof JsonArray) {
							add((JsonArray) valueObject);
						} else if (valueObject instanceof String) {
							add((String) valueObject);
						} else if (valueObject instanceof Number) {
							add((Number) valueObject);
						} else if (valueObject instanceof Boolean) {
							add((Boolean) valueObject);
						}

						valueObject = null;
					} else if (charItem == ',') {
						throw new Exception("Unexpected '" + charItem + "'-sign at index " + index);
					}

					if (charItem == ']') {
						return index;
					}
				} else if (charItem == '[') {
					JsonArray newJsonArray = new JsonArray();
					index = newJsonArray.parse(jsonData, index + 1);
					valueObject = newJsonArray;
				} else if (charItem == '{') {
					JsonObject newJsonObject = new JsonObject();
					index = newJsonObject.parse(jsonData, index + 1);
					valueObject = newJsonObject;
				} else if (charItem == '}') {
					throw new Exception("Unexpected '" + charItem + "'-sign at index " + index);
				} else if (charItem == '"') {
					quotedValue = new StringBuilder();
				} else {
					if (unquotedValue == null) {
						unquotedValue = new StringBuilder();
					}
					unquotedValue.append(charItem);
					if (!JsonUtilities.isValidUnfinishedJsonValue(unquotedValue.toString())) {
						throw new Exception("Unexpected '" + charItem + "'-sign at index " + index);
					}
				}

				previousChar = charItem;
			}

			throw new Exception("Unclosed object value");
		} catch (Exception e) {
			throw new Exception("Invalid JSON data starting at index " + startIndex, e);
		}
	}
}
