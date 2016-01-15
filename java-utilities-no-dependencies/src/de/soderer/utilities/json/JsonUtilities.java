package de.soderer.utilities.json;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import de.soderer.utilities.Utilities;

public class JsonUtilities {
	public static JsonObject parseJsonData(String jsonDataString, boolean throwExceptionOnError) throws Exception {
		try {
			char[] jsonData = jsonDataString.toCharArray();
			int startIndex = 0;
			for (startIndex = 0; startIndex < jsonData.length; startIndex++) {
				if (Character.isWhitespace(jsonData[startIndex])) {
					// do nothing
				} else if (jsonData[startIndex] == '{') {
					break;
				} else {
					throw new Exception("Unexpected '" + jsonData[startIndex] + "'-sign at index " + startIndex);
				}
			}

			JsonObject jsonObject = new JsonObject();
			int endIndexOfObject = jsonObject.parse(jsonData, startIndex + 1) + 1;
			if (endIndexOfObject < jsonData.length) {
				for (int index = endIndexOfObject; index < jsonData.length; index++) {
					if (!Character.isWhitespace(jsonData[index])) {
						throw new Exception("Unexpected '" + jsonData[index] + "'-sign at index " + index);
					}
				}
			}

			return jsonObject;
		} catch (Exception e) {
			if (throwExceptionOnError) {
				throw new Exception("Invalid JSON data", e);
			} else {
				return null;
			}
		}
	}

	public static boolean isValidUnfinishedJsonValue(String value) {
		if ("t".equalsIgnoreCase(value) || "tr".equalsIgnoreCase(value) || "tru".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value)) {
			return true;
		} else if ("f".equalsIgnoreCase(value) || "fa".equalsIgnoreCase(value) || "fal".equalsIgnoreCase(value) || "fals".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
			return true;
		} else if ("n".equalsIgnoreCase(value) || "nu".equalsIgnoreCase(value) || "nul".equalsIgnoreCase(value) || "null".equalsIgnoreCase(value)) {
			return true;
		} else if (Pattern.matches("[+|-]?[0-9]*(\\.[0-9]*)?([e|E][+|-]?[0-9]*)?", value)) {
			return true;
		} else {
			return false;
		}
	}

	public static Object getJsonValue(String value) throws Exception {
		if ("true".equalsIgnoreCase(value)) {
			return true;
		} else if ("false".equalsIgnoreCase(value)) {
			return false;
		} else if ("null".equalsIgnoreCase(value)) {
			return null;
		} else if (Pattern.matches("[+|-]?[0-9]*(\\.[0-9]*)?([e|E][+|-]?[0-9]*)?", value)) {
			if (value.contains(".")) {
				return new Double(value);
			} else {
				return new Long(value);
			}
		} else {
			throw new Exception("Invalid value");
		}
	}

	public static JsonObject convertXmlDocument(Document xmlDocument, boolean throwExceptionOnError) throws Exception {
		try {
			JsonObject jsonObject = new JsonObject();
			jsonObject.add(xmlDocument.getChildNodes().item(0).getNodeName(), convertXmlNode(xmlDocument.getChildNodes().item(0)));
			return jsonObject;
		} catch (Exception e) {
			if (throwExceptionOnError) {
				throw new Exception("Invalid data", e);
			} else {
				return null;
			}
		}
	}

	public static JsonObject convertXmlNode(Node xmlNode) {
		JsonObject jsonObject = new JsonObject();
		if (xmlNode.getAttributes() != null && xmlNode.getAttributes().getLength() > 0) {
			for (int attributeIndex = 0; attributeIndex < xmlNode.getAttributes().getLength(); attributeIndex++) {
				Node attributeNode = xmlNode.getAttributes().item(attributeIndex);
				jsonObject.add(attributeNode.getNodeName(), attributeNode.getNodeValue());
			}
		}
		if (xmlNode.getChildNodes() != null && xmlNode.getChildNodes().getLength() > 0) {
			for (int i = 0; i < xmlNode.getChildNodes().getLength(); i++) {
				Node childNode = xmlNode.getChildNodes().item(i);
				if (childNode.getNodeType() == Node.TEXT_NODE) {
					if (Utilities.isNotBlank(childNode.getNodeValue())) {
						jsonObject.add("text", childNode.getNodeValue());
					}
				} else if (childNode.getNodeType() == Node.COMMENT_NODE) {
					// do nothing
				} else if (childNode.getChildNodes().getLength() == 1 && childNode.getChildNodes().item(0).getNodeType() == Node.TEXT_NODE) {
					// only one textnode under this node
					jsonObject.add(childNode.getNodeName(), childNode.getChildNodes().item(0).getNodeValue());
				} else {
					Node xmlSubNode = childNode;
					JsonObject nodeJsonObject = convertXmlNode(xmlSubNode);
					if (nodeJsonObject != null) {
						jsonObject.add(xmlSubNode.getNodeName(), nodeJsonObject);
					}
				}
			}
		}
		return jsonObject;
	}

	public static Document convertToXmlDocument(JsonObject jsonObject, boolean useAttributes) throws Exception {
		try {
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			Document xmlDocument = documentBuilder.newDocument();
			xmlDocument.setXmlStandalone(true);
			List<Node> mainNodes = convertToXmlNodes(jsonObject, xmlDocument, useAttributes);
			if (mainNodes == null || mainNodes.size() < 1) {
				throw new Exception("No data found");
			} else if (mainNodes.size() == 1) {
				xmlDocument.appendChild(mainNodes.get(0));
			} else {
				Node rootNode = xmlDocument.createElement("root");
				for (Node subNode : mainNodes) {
					if (subNode instanceof Attr) {
						rootNode.getAttributes().setNamedItem(subNode);
					} else {
						rootNode.appendChild(subNode);
					}
				}
				xmlDocument.appendChild(rootNode);
			}
			return xmlDocument;
		} catch (Exception e) {
			throw new Exception("Invalid data", e);
		}
	}

	public static List<Node> convertToXmlNodes(JsonObject jsonObject, Document xmlDocument, boolean useAttributes) {
		List<Node> list = new ArrayList<Node>();

		for (String key : jsonObject.keySet()) {
			Object subItem = jsonObject.get(key);
			if (subItem instanceof JsonObject) {
				Node newNode = xmlDocument.createElement(key);
				list.add(newNode);
				for (Node subNode : convertToXmlNodes((JsonObject) subItem, xmlDocument, useAttributes)) {
					if (subNode instanceof Attr) {
						newNode.getAttributes().setNamedItem(subNode);
					} else {
						newNode.appendChild(subNode);
					}
				}
			} else if (subItem instanceof JsonArray) {
				for (Node subNode : convertToXmlNodes((JsonArray) subItem, key, xmlDocument, useAttributes)) {
					list.add(subNode);
				}
			} else if (useAttributes) {
				Attr newAttr = xmlDocument.createAttribute(key);
				newAttr.setNodeValue(subItem.toString());
				list.add(newAttr);
			} else {
				Node newNode = xmlDocument.createElement(key);
				list.add(newNode);
				newNode.setTextContent(subItem.toString());
			}
		}

		return list;
	}

	public static List<Node> convertToXmlNodes(JsonArray jsonArray, String nodeName, Document xmlDocument, boolean useAttributes) {
		List<Node> list = new ArrayList<Node>();

		if (jsonArray.size() > 0) {
			for (Object subItem : jsonArray) {
				if (subItem instanceof JsonObject) {
					Node newNode = xmlDocument.createElement(nodeName);
					list.add(newNode);
					for (Node subNode : convertToXmlNodes((JsonObject) subItem, xmlDocument, useAttributes)) {
						if (subNode instanceof Attr) {
							newNode.getAttributes().setNamedItem(subNode);
						} else {
							newNode.appendChild(subNode);
						}
					}
				} else if (subItem instanceof JsonArray) {
					Node newNode = xmlDocument.createElement(nodeName);
					list.add(newNode);
					for (Node subNode : convertToXmlNodes((JsonArray) subItem, nodeName, xmlDocument, useAttributes)) {
						newNode.appendChild(subNode);
					}
				} else {
					Node newNode = xmlDocument.createElement(nodeName);
					list.add(newNode);
					newNode.setTextContent(subItem.toString());
				}
			}
		} else {
			Node newNode = xmlDocument.createElement(nodeName);
			list.add(newNode);
		}

		return list;
	}
}
