package org.ohdsi.rabbitInAHat;

import java.util.List;

import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.ItemToItemMap;
import org.ohdsi.rabbitInAHat.dataModel.MappableItem;
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
		List<MappableItem> list = tableMap.getTargetItems();
		for (MappableItem targetTable : list) {
			
			List<MappableItem> sourceList = tableMap.getSourceItemsFromTarget(targetTable);
			
			
			for (MappableItem sourceTable : sourceList) {
				Mapping<Field> fieldMapping = ObjectExchange.etl.getFieldToFieldMapping((Table) sourceTable, (Table) targetTable);
				
				List<Field> fields = castToTable(targetTable).getFields();
				
				for (Field targetField : fields) {
					List<MappableItem> sourceFields = fieldMapping.getSourceItemsFromTarget(targetField);
					
					Field initialSourceField = castToField(sourceFields.get(0));
					result += "INSERT INTO " + targetTable.getName() + " (" + targetField.getName() + ")\n";
					result += "SELECT " + initialSourceField.getName() + " AS " + targetField.getName() + "\n";
					result += "FROM " + initialSourceField.getTable().getName() + ";\n\n";
				}
			}
			
		}
		return result;
	}
	
	private static Field castToField (MappableItem item) {
		if (item instanceof Field) {
			return (Field) item;
		}
		else {
			throw new IllegalArgumentException();
		}
	}
	
	private static Table castToTable (MappableItem item) {
		if (item instanceof Table) {
			return (Table) item;
		}
		else {
			throw new IllegalArgumentException();
		}
	}
}
