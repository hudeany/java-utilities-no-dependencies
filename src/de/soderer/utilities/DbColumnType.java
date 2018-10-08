package de.soderer.utilities;

public class DbColumnType {
	public enum SimpleDataType {
		String, Date, Integer, Double, Blob, Clob
	}

	private String typeName;
	private long characterByteSize; // only for VARCHAR and VARCHAR2 types
	private int numericPrecision; // only for numeric types
	private int numericScale; // only for numeric types
	private boolean nullable;
	private boolean autoIncrement;

	public DbColumnType(String typeName, long characterByteSize, int numericPrecision, int numericScale, boolean nullable, boolean autoIncrement) {
		this.typeName = typeName;
		this.characterByteSize = characterByteSize;
		this.numericPrecision = numericPrecision;
		this.numericScale = numericScale;
		this.nullable = nullable;
		this.autoIncrement = autoIncrement;
	}

	public String getTypeName() {
		return typeName;
	}

	public long getCharacterByteSize() {
		return characterByteSize;
	}

	public int getNumericPrecision() {
		return numericPrecision;
	}

	public int getNumericScale() {
		return numericScale;
	}

	public boolean isNullable() {
		return nullable;
	}

	public boolean isAutoIncrement() {
		return autoIncrement;
	}

	public SimpleDataType getSimpleDataType() {
		if (typeName.toLowerCase().startsWith("date") || typeName.toLowerCase().startsWith("time")) {
			return SimpleDataType.Date;
		} else if (typeName.toLowerCase().equals("clob") || typeName.toLowerCase().equals("longtext")) {
			return SimpleDataType.Clob;
		} else if (typeName.toLowerCase().startsWith("varchar") || typeName.toLowerCase().contains("text") || typeName.toLowerCase().startsWith("character")) {
			return SimpleDataType.String;
		} else if (typeName.toLowerCase().equals("blob") || typeName.toLowerCase().equals("bytea")) {
			return SimpleDataType.Blob;
		} else if (typeName.toLowerCase().contains("int")) {
			return SimpleDataType.Integer;
		} else {
			// e.g.: PostgreSQL "REAL"
			return SimpleDataType.Double;
		}
	}
	
	@Override
	public String toString() {
		SimpleDataType simpleDataType = getSimpleDataType();
		return typeName
				+ (simpleDataType == SimpleDataType.String ? "(" + characterByteSize + ")" : "")
				+ (simpleDataType == SimpleDataType.Integer || simpleDataType == SimpleDataType.Double ? "(" + numericPrecision + ", " + numericScale + ")" : "")
				+ (nullable ? " nullable": " not nullable")
				+ (autoIncrement ? " autoIncrement": "");
	}
}
