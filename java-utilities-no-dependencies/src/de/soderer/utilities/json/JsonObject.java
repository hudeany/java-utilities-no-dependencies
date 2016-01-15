package de.soderer.utilities.json;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import de.soderer.utilities.TextUtilities;

public class JsonObject {
	private Map<String, Object> properties = new LinkedHashMap<String, Object>();

	public void add(String key, JsonObject object) {
		add(key, (Object) object);
	}

	public void add(String key, JsonArray array) {
		add(key, (Object) array);
	}

	public void add(String key, String value) {
		add(key, (Object) value);
	}

	public void add(String key, Number value) {
		add(key, (Object) value);
	}

	public void add(String key, Boolean value) {
		add(key, (Object) value);
	}

	public void addNullValue(String key) {
		add(key, (Object) null);
	}

	private void add(String key, Object object) {
		if (properties.containsKey(key)) {
			Object oldObject = properties.get(key);
			if (oldObject instanceof JsonArray) {
				if (object == null) {
					((JsonArray) oldObject).addNullValue();
				} else if (object instanceof JsonObject) {
					((JsonArray) oldObject).add((JsonObject) object);
				} else if (object instanceof JsonArray) {
					((JsonArray) oldObject).add((JsonArray) object);
				} else if (object instanceof String) {
					((JsonArray) oldObject).add((String) object);
				} else if (object instanceof Number) {
					((JsonArray) oldObject).add((Number) object);
				} else if (object instanceof Boolean) {
					((JsonArray) oldObject).add((Boolean) object);
				}
			} else {
				JsonArray replacementArray = new JsonArray();
				properties.put(key, replacementArray);
				if (oldObject == null) {
					replacementArray.addNullValue();
				} else if (oldObject instanceof JsonObject) {
					replacementArray.add((JsonObject) oldObject);
				} else if (oldObject instanceof JsonArray) {
					replacementArray.add((JsonArray) oldObject);
				} else if (oldObject instanceof String) {
					replacementArray.add((String) oldObject);
				} else if (oldObject instanceof Number) {
					replacementArray.add((Number) oldObject);
				} else if (oldObject instanceof Boolean) {
					replacementArray.add((Boolean) oldObject);
				}
				if (object == null) {
					replacementArray.addNullValue();
				} else if (object instanceof JsonObject) {
					replacementArray.add((JsonObject) object);
				} else if (object instanceof JsonArray) {
					replacementArray.add((JsonArray) object);
				} else if (object instanceof String) {
					replacementArray.add((String) object);
				} else if (object instanceof Number) {
					replacementArray.add((Number) object);
				} else if (object instanceof Boolean) {
					replacementArray.add((Boolean) object);
				}
			}
		} else {
			properties.put(key, object);
		}
	}

	public Object remove(String key) {
		return properties.remove(key);
	}

	public Object get(String key) {
		return properties.get(key);
	}

	public Set<String> keySet() {
		return properties.keySet();
	}

	public int size() {
		return properties.size();
	}

	@Override
	public String toString() {
		return toString("\t", " ", "\n");
	}

	public String toString(String indention, String separator, String linebreak) {
		if (properties.size() > 0) {
			boolean isFirstKey = true;
			StringBuilder result = new StringBuilder("{");

			for (Object keyObject : properties.keySet()) {
				if (!isFirstKey) {
					result.append(",");
				}

				result.append(linebreak);
				String key = (String) keyObject;
				result.append(indention);
				result.append("\"");
				result.append(key.replace("\"", "\\\""));
				result.append("\":");
				result.append(separator);
				Object subItem = properties.get(key);
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

				isFirstKey = false;
			}

			result.append(linebreak);
			result.append("}");
			result.append(linebreak);

			return result.toString();
		} else {
			return "{}";
		}
	}

	protected int parse(char[] jsonData, int startIndex) throws Exception {
		try {
			properties.clear();

			char previousChar = ' ';
			StringBuilder key = null;
			StringBuilder quotedValue = null;
			StringBuilder unquotedValue = null;
			String keyString = null;
			Object valueObject = null;

			for (int index = startIndex; index < jsonData.length; index++) {
				char charItem = jsonData[index];

				if (charItem == '\\') {
					// keep for following char check
					if (key == null && quotedValue == null) {
						throw new Exception("Unexpected '" + charItem + "'-sign at index " + index);
					}
				} else if (key != null) {
					if (charItem == '"' && previousChar != '\\') {
						keyString = key.toString();
						key = null;
					} else if (previousChar == '\\') {
						key.append('\\');
						key.append(charItem);
					} else {
						key.append(charItem);
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
					if (keyString == null) {
						throw new Exception("Unexpected ':'-sign at index " + index);
					}
				} else if (charItem == ',' || charItem == '}') {
					// end of value for key or array item
					if (keyString != null && (valueObject != null || unquotedValue != null)) {
						if (unquotedValue != null) {
							valueObject = JsonUtilities.getJsonValue(unquotedValue.toString());
							unquotedValue = null;
						}

						if (valueObject == null) {
							addNullValue(keyString);
						} else if (valueObject instanceof JsonObject) {
							add(keyString, (JsonObject) valueObject);
						} else if (valueObject instanceof JsonArray) {
							add(keyString, (JsonArray) valueObject);
						} else if (valueObject instanceof String) {
							add(keyString, (String) valueObject);
						} else if (valueObject instanceof Number) {
							add(keyString, (Number) valueObject);
						} else if (valueObject instanceof Boolean) {
							add(keyString, (Boolean) valueObject);
						}

						keyString = null;
						valueObject = null;
					} else if (charItem == ',') {
						throw new Exception("Unexpected '" + charItem + "'-sign at index " + index);
					}

					if (charItem == '}') {
						return index;
					}
				} else if (charItem == '{') {
					JsonObject newJsonObject = new JsonObject();
					index = newJsonObject.parse(jsonData, index + 1);
					valueObject = newJsonObject;
				} else if (charItem == '[') {
					JsonArray newJsonArray = new JsonArray();
					index = newJsonArray.parse(jsonData, index + 1);
					valueObject = newJsonArray;
				} else if (charItem == ']') {
					throw new Exception("Unexpected '" + charItem + "'-sign at index " + index);
				} else if (charItem == '"') {
					if (keyString == null) {
						key = new StringBuilder();
					} else {
						quotedValue = new StringBuilder();
					}
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
