package de.soderer.utilities;

public class SimpleCredentialsData {
	private String userName;
	private char[] password;

	protected SimpleCredentialsData() {
	}

	public SimpleCredentialsData(String userName, char[] password) {
		this.userName = userName;
		this.password = password;
	}

	public String getUserName() {
		return userName;
	}

	public char[] getPassword() {
		return password;
	}
}
