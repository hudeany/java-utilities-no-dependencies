package de.soderer.utilities;

public class ServerAccessData {
	public static final String PRELUDE = "\\\\";

	private String hostname;
	private int port;
	private String username;
	private String keyFile;
	private String password;
	private boolean checkHostKey;

	protected ServerAccessData() {
	}

	public ServerAccessData(String hostname, String username, String keyFile, String password) {
		this(hostname, 22, username, keyFile, password, false);
	}

	public ServerAccessData(String hostname, int port, String username, String keyFile, String password, boolean checkHostKey) {
		this.hostname = hostname;
		this.port = port;
		this.username = username;
		this.keyFile = keyFile;
		this.password = password;
		this.checkHostKey = checkHostKey;
	}

	public String getHostname() {
		return hostname;
	}

	public int getPort() {
		return port;
	}

	public String getUsername() {
		return username;
	}

	public String getKeyFile() {
		return keyFile;
	}

	public String getPassword() {
		return password;
	}

	public boolean getCheckHostKey() {
		return checkHostKey;
	}

	public static String parseFilestringToServerEntryName(String fileName) throws Exception {
		if (Utilities.isNotBlank(fileName) && fileName.startsWith(PRELUDE) && fileName.contains(":")) {
			return fileName.substring(PRELUDE.length(), fileName.lastIndexOf(":"));
		} else {
			throw new Exception("Invalid SSH-filename");
		}
	}

	public static String parseFilestringToFilePath(String fileName) throws Exception {
		if (Utilities.isNotBlank(fileName) && fileName.startsWith(PRELUDE) && fileName.contains(":")) {
			return fileName.substring(fileName.lastIndexOf(":") + 1);
		} else {
			throw new Exception("Invalid SSH-filename");
		}
	}
}