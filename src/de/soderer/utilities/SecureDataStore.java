package de.soderer.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.JsonReader;
import de.soderer.utilities.json.JsonReader.JsonToken;
import de.soderer.utilities.json.JsonSerializer;
import de.soderer.utilities.json.JsonWriter;

public class SecureDataStore {
	private static final String SYMMETRIC_ENCRYPTION_METHOD = "AES";
	
	/**
	 * First key is class name of entries grouped by their class name, second key is the entry name
	 */
	private Map<String, Map<String, Object>> dataEntries = new HashMap<String, Map<String, Object>>();

	public void save(File storeFile, char[] password, byte[] salt) throws Exception {
		try {
			if (storeFile == null) {
				throw new Exception("SecureDataStore file is undefined");
			} else {
				if (password == null) {
					password = "".toCharArray();
				}
				try (FileOutputStream fos = new FileOutputStream(storeFile)) {
					byte[] keyBytes = stretchPassword(password, salt);
					SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");
					Cipher encryptCipher = Cipher.getInstance(SYMMETRIC_ENCRYPTION_METHOD);
					encryptCipher.init(Cipher.ENCRYPT_MODE, keySpec);
					try (CipherOutputStream cipherOutputStream = new CipherOutputStream(fos, encryptCipher)) {
						try (JsonWriter jsonWriter = new JsonWriter(cipherOutputStream)) {
							jsonWriter.openJsonArray();
							for (Entry<String, Map<String, Object>> entryMap : dataEntries.entrySet()) {
								for (Entry<String, Object> entry : entryMap.getValue().entrySet()) {
									JsonObject entryObject = new JsonObject();
									entryObject.add("class", entryMap.getKey());
									entryObject.add("name", entry.getKey());
									entryObject.add("value", JsonSerializer.serialize(entry.getValue(), false, false, false, true).getValue());
									jsonWriter.add(entryObject);
								}
							}
							jsonWriter.closeJsonArray();
						}
					}
				}
			}
		} finally {
			Arrays.fill(password, '\u0000');
		}
	}
	
	public void load(File storeFile, char[] password, byte[] salt) throws Exception {
		try {
			if (storeFile == null) {
				throw new Exception("SecureDataStore file is undefined");
			} else if (!storeFile.exists()) {
				throw new Exception("SecureDataStore file does not exist: " + storeFile.getAbsolutePath());
			} else {
				if (password == null) {
					password = "".toCharArray();
				}
				dataEntries = new HashMap<String, Map<String, Object>>();
				try (FileInputStream fis = new FileInputStream(storeFile)) {
					byte[] keyBytes = stretchPassword(password, salt);
					SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");
					Cipher decryptCipher = Cipher.getInstance(SYMMETRIC_ENCRYPTION_METHOD);
					decryptCipher.init(Cipher.DECRYPT_MODE, keySpec);
					try (CipherInputStream cipherInputStream = new CipherInputStream(fis, decryptCipher)) {
						try (JsonReader jsonWriter = new JsonReader(cipherInputStream)) {
							JsonToken jsonToken = jsonWriter.readNextToken();
							if (jsonToken != JsonToken.JsonArray_Open) {
								throw new Exception("SecureDataStore is corrupt");
							} else {
								while (jsonWriter.readNextJsonNode()) {
									if (jsonToken == JsonToken.JsonArray_Close) {
										break;
									} else {
										JsonObject entryObject = (JsonObject) jsonWriter.getCurrentObject();
										String className = (String) entryObject.get("class");
										if (!dataEntries.containsKey(className)) {
											dataEntries.put(className, new HashMap<String, Object>());
										}
										String entryName = (String) entryObject.get("name");
										dataEntries.get(className).put(entryName, JsonSerializer.deserialize((JsonObject) entryObject.get("value")));
									}
								}
							}
						}
					} catch (IOException e) {
						if (e.getCause() != null && e.getCause() instanceof BadPaddingException) {
							throw new WrongPasswordException();
						} else {
							throw e;
						}
					}
				}
			}
		} finally {
			Arrays.fill(password, '\u0000');
		}
	}

	private byte[] stretchPassword(char[] password, byte[] salt) throws NoSuchAlgorithmException, InvalidKeySpecException {
		String algorithm = "PBKDF2WithHmacSHA512";
		int derivedKeyLength = 128;
		int iterations = 5000;
		KeySpec spec = new PBEKeySpec(password, salt, iterations, derivedKeyLength);
		SecretKeyFactory f = SecretKeyFactory.getInstance(algorithm);
		return f.generateSecret(spec).getEncoded();
	}

	public Set<String> getEntryNames(Class<?> classType) {
		if (dataEntries.get(classType.getName()) == null) {
			return new HashSet<String>();
		} else {
			return dataEntries.get(classType.getName()).keySet();
		}
	}

	public <T> T getEntry(Class<T> classType, String entryName) {
		if (dataEntries.get(classType.getName()) == null) {
			return null;
		}
		@SuppressWarnings("unchecked")
		T returnValue = (T) dataEntries.get(classType.getName()).get(entryName);
		return returnValue;
	}

	public Object getEntry(String entryName) {
		for (Entry<String, Map<String, Object>> classEntry : dataEntries.entrySet()) {
			for (Entry<String, Object> entry : classEntry.getValue().entrySet()) {
				if (StringUtilities.equals(entryName, entry.getKey())) {
					return entry.getValue();
				}
			}
		}
		return null;
	}

	public void addEntry(String entryName, Object entryValue) {
		if (dataEntries.get(entryValue.getClass().getName()) == null) {
			dataEntries.put(entryValue.getClass().getName(), new HashMap<String, Object>());
		}
		dataEntries.get(entryValue.getClass().getName()).put(entryName, entryValue);
	}

	public void removeEntry(Class<?> classType, String entryName) {
		dataEntries.get(classType.getName()).remove(entryName);
	}

	public void removeEntriesByEntryName(String entryName) {
		for (Entry<String, Map<String, Object>> classEntry : dataEntries.entrySet()) {
			classEntry.getValue().remove(entryName);
		}
	}

	public void removeEntriesByEntryClass(Class<?> classType) {
		dataEntries.remove(classType.getName());
	}
}
