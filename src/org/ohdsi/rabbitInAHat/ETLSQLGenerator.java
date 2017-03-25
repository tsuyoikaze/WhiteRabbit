package org.ohdsi.rabbitInAHat;

import java.util.List;

import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.Mapping;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.whiteRabbit.ObjectExchange;

public class ETLSQLGenerator {
	
	public static enum DataTypeFormat {
		MySQL;
	}
	
	public static String convertDataType(String rawType, DataTypeFormat format) {
		if (format == DataTypeFormat.MySQL) {
			switch (rawType) {
			case "INTEGER":
				return "INT";
			case "CHARACTER VARYING":
				return "varchar(45)";
			}
			return rawType;
		}
		
		return rawType;
	}
	
	public static String getCreateTable () {
		List<Table> tables = ObjectExchange.etl.getTargetDatabase().getTables();
		String result = "";
		for (Table table : tables) {
			result += "DROP TABLE IF EXISTS " + table.getName() + ";\n";
			result += "CREATE TABLE " + table.getName() + " (\n";
			List<Field> fields = table.getFields();
			for (int i = 0; i < fields.size(); i++) {
				Field f = fields.get(i);
				result += "\t" + f.getName() + " " + convertDataType(f.getType(), DataTypeFormat.MySQL) + " " + (f.isNullable() ? "NULL" : "NOT NULL") + (i == fields.size() - 1 ? "" : ",") + "\n";
			}
			result += ")\n\n";
		}
		
		return result;
	}
	
	public static String getMap () {
		String result = "";
		Mapping<Table> tableMap = ObjectExchange.etl.getTableToTableMapping();
		return result;
	}
}
