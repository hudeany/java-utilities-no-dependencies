package de.soderer.utilities;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public abstract class BasicReader implements Closeable {
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
	private long readCharacters = 0;

	public BasicReader(InputStream inputStream) {
		this(inputStream, null);
	}
	
	public long getReadCharacters() {
		return readCharacters;
	}
	
	public BasicReader(InputStream inputStream, String encoding) {
		this.inputStream = inputStream;
		this.encoding = isBlank(encoding) ? Charset.forName(DEFAULT_ENCODING) : Charset.forName(encoding);
	}
	
	public void reuseCurrentChar() {
		reuseChar = currentChar;
		readCharacters--;
	}

	protected char readNextCharacter() throws Exception {
		if (inputReader == null) {
			inputReader = new BufferedReader(new InputStreamReader(inputStream, encoding));
		}
		
		if (reuseChar != null) {
			currentChar = reuseChar;
			reuseChar = null;
			readCharacters++;
			return currentChar;
		} else {
			int currentCharInt = inputReader.read();
			if (currentCharInt != -1) {
				currentChar = (char) currentCharInt;
				readCharacters++;
				return currentChar;
			} else {
				throw new Exception("Premature end of data");
			}
		}
	}

	protected char readNextNonWhitespace() throws Exception {
		readNextCharacter();
		while (Character.isWhitespace(currentChar) ) {
			readNextCharacter();
		}
		return currentChar;
	}

	protected String readUpToNext(boolean includeLimitChars, Character escapeCharacter, char... endChars) throws Exception {
		if (Utilities.anyCharsAreEqual(endChars)) {
			throw new Exception("Invalid limit characters");
		} else if (Utilities.contains(endChars, escapeCharacter)) {
			throw new Exception("Invalid escape characters");
		}
		
		StringBuilder returnValue = new StringBuilder();
		returnValue.append(currentChar);
		boolean escapeNextCharacter = false;
		while (true) {
			readNextCharacter();
			if (!escapeNextCharacter) {
				if (escapeCharacter != null && escapeCharacter == currentChar) {
					escapeNextCharacter = true;
				} else {
					for (char endChar : endChars) {
						if (endChar == currentChar) {
							if (includeLimitChars) {
								returnValue.append(currentChar);
							} else {
								reuseCurrentChar();
							}
							return returnValue.toString();
						}
					}
					returnValue.append(currentChar);
				}
			} else {
				if (escapeCharacter == currentChar) {
					returnValue.append(escapeCharacter);
				} else if ('\n' == currentChar) {
					returnValue.append('\n');
				} else {
					for (char endChar : endChars) {
						if (endChar == currentChar) {
							returnValue.append(endChar);
						}
					}
				}
				escapeNextCharacter = false;
			}
		}
	}

	protected String readQuotedText(char quoteChar, Character escapeCharacter) throws Exception {
		if (currentChar != quoteChar) {
			throw new Exception("Invalid start of double-quoted text");
		}
		
		String returnValue = readUpToNext(true, escapeCharacter, quoteChar);
		return returnValue.substring(1, returnValue.length() - 1);
	}

	/**
	 * Close this writer and its underlying stream.
	 */
	@Override
	public void close() throws IOException {
		closeQuietly(inputReader);
		inputReader = null;
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
}
