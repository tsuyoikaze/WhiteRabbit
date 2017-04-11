package org.ohdsi.rabbitInAHat.dataModel;

import java.io.Serializable;
import java.util.regex.Pattern;

import org.ohdsi.databases.DbType;
import org.ohdsi.utilities.exception.UnsupportedDataTypeException;
import org.ohdsi.utilities.exception.UnsupportedDatabaseException;

public enum DataType implements Serializable {
	VARCHAR, INTEGER, FLOAT, DATE, DATETIME, TIMESTAMP, TIME, YEAR, TEXT, UNKNOWN;
	
	private static final Pattern STR_PATTERN = Pattern.compile("str|varchar|character", Pattern.CASE_INSENSITIVE);
	private static final Pattern TEXT_PATTERN = Pattern.compile("text", Pattern.CASE_INSENSITIVE);
	private static final Pattern INT_PATTERN = Pattern.compile("int|num", Pattern.CASE_INSENSITIVE);
	private static final Pattern FLOAT_PATTERN = Pattern.compile("double|float|decimal", Pattern.CASE_INSENSITIVE);
	private static final Pattern DATE_PATTERN = Pattern.compile("date", Pattern.CASE_INSENSITIVE);
	private static final Pattern TIME_PATTERN = Pattern.compile("time", Pattern.CASE_INSENSITIVE);
	private static final Pattern DATETIME_PATTERN = Pattern.compile("datetime", Pattern.CASE_INSENSITIVE);
	private static final Pattern YEAR_PATTERN = Pattern.compile("year", Pattern.CASE_INSENSITIVE);
	private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("timestamp", Pattern.CASE_INSENSITIVE);
	
	public String toString () {
		switch (this) {
		case VARCHAR:
			return "VARCHAR";
		case TEXT:
			return "TEXT";
		case INTEGER:
			return "INTEGER";
		case FLOAT:
			return "FLOAT";
		case DATE:
			return "DATE";
		case DATETIME:
			return "DATETIME";
		case TIME:
			return "TIME";
		case YEAR:
			return "YEAR";
		case TIMESTAMP:
			return "TIMESTAMP";
		default:
			return "UNSUPPORTED TYPE";
		}
	}
	
	public String toCreateString (DbType type) {
		if (type.equals(DbType.MYSQL)) {
			switch (this) {
			case VARCHAR:
				return "VARCHAR(45)";
			case TEXT:
				return "LONGTEXT";
			case INTEGER:
				return "INT(11)";
			case FLOAT:
				return "FLOAT";
			case DATE:
				return "DATE";
			case DATETIME:
				return "DATETIME";
			case TIME:
				return "TIME";
			case YEAR:
				return "YEAR";
			case TIMESTAMP:
				return "TIMESTAMP";
			default:
				return "UNSUPPORTED TYPE";
			}
		}
		throw new UnsupportedDatabaseException();
	}
	
	public static DataType fromString (String str) {
		if (STR_PATTERN.matcher(str).find()) {
			return DataType.VARCHAR;
		}
		else if (TEXT_PATTERN.matcher(str).find()) {
			return DataType.TEXT;
		}
		else if (INT_PATTERN.matcher(str).find()) {
			return DataType.INTEGER;
		}
		else if (FLOAT_PATTERN.matcher(str).find()) {
			return DataType.FLOAT;
		}
		else if (DATETIME_PATTERN.matcher(str).find()) {
			return DataType.FLOAT;
		}
		else if (DATE_PATTERN.matcher(str).find()) {
			return DataType.DATE;
		}
		else if (TIME_PATTERN.matcher(str).find()) {
			return DataType.TIME;
		}
		else if (TIME_PATTERN.matcher(str).find()) {
			return DataType.TIME;
		}
		else if (TIMESTAMP_PATTERN.matcher(str).find()) {
			return DataType.TIMESTAMP;
		}
		else if (YEAR_PATTERN.matcher(str).find()) {
			return DataType.YEAR;
		}
		else {
			return DataType.UNKNOWN;
		}
	}
}
