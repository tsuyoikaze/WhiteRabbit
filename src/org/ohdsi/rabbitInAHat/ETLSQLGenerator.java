package org.ohdsi.rabbitInAHat;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ohdsi.databases.DbType;
import org.ohdsi.rabbitInAHat.dataModel.DataType;
import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.ItemToItemMap;
import org.ohdsi.rabbitInAHat.dataModel.MappableItem;
import org.ohdsi.rabbitInAHat.dataModel.Mapping;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.utilities.collections.Pair;
import org.ohdsi.utilities.exception.DuplicateTargetException;
import org.ohdsi.utilities.exception.TypeMismatchException;
import org.ohdsi.whiteRabbit.ObjectExchange;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Collections;

public class ETLSQLGenerator {
	
	public static final String HEADER_COMMENT = "# AUTOMATICALLY GENERATED BY RabbitInAHat\n# MUST USE WITH THE SAME DATABASE AS THE SOURCE TABLES\n\n";
	public static final HashMap<DataType, DataType> DIRECTLY_CONVERTABLE_TYPES = new HashMap<>();
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
				result += "\t" + f.getName() + " " + f.getType().toCreateString(DbType.MYSQL) +/* " " + (f.isNullable() ? "NULL" : "NOT NULL") +*/ (i == fields.size() - 1 ? "" : ",") + "\n";
			}
			result += ");\n\n";
		}
		
		return result;
	}
	
	public static String getMap () throws TypeMismatchException, DuplicateTargetException {
		
		List<ItemToItemMap> fieldMaps = new ArrayList<>();
		
		for (Table sourceTable : ObjectExchange.etl.getSourceDatabase().getTables()) {
			for (Table targetTable : ObjectExchange.etl.getTargetDatabase().getTables()) {
				fieldMaps.addAll(ObjectExchange.etl.getFieldToFieldMapping(sourceTable, targetTable).getSourceToTargetMaps());
			}
		}
		
		if (!checkDuplicate(fieldMaps)) {
			throw new DuplicateTargetException();
		}
		
		if (!checkDataType(fieldMaps)) {
			throw new TypeMismatchException();
		}
		
		String result = "";
		Mapping<Table> tableMap = ObjectExchange.etl.getTableToTableMapping();
		List<MappableItem> list = tableMap.getTargetItems();
		for (MappableItem targetTable : list) {
			
			List<MappableItem> sourceList = tableMap.getSourceItemsFromTarget(targetTable);
			
			
			for (MappableItem sourceTable : sourceList) {
				Mapping<Field> fieldMapping = ObjectExchange.etl.getFieldToFieldMapping((Table) sourceTable, (Table) targetTable);
				
				List<Field> fields = castToTable(targetTable).getFields();
				sort(fields);
				
				boolean isFirstItem = true;
				Field initialSourceField = null;
				Field initialTargetField = null;
				
				for (Field targetField : fields) {
					
					if (isFirstItem) {
						
						List<MappableItem> sourceFields = fieldMapping.getSourceItemsFromTarget(targetField);
						initialSourceField = castToField(sourceFields.get(0));
						initialTargetField = targetField;
						
						result += "INSERT INTO " + targetTable.getName() + " (" + targetField.getName() + ")\n";
						result += "SELECT " + initialSourceField.getName() + " AS " + targetField.getName() + "\n";
						result += "FROM " + initialSourceField.getTable().getName() + ";\n\n";
						
						isFirstItem = false;
						continue;
					}
					
					List<MappableItem> sourceFields = fieldMapping.getSourceItemsFromTarget(targetField);
					
					if (sourceFields.isEmpty()) {
						continue;
					}
					
					Field sourceField = castToField(sourceFields.get(0));
					result += "UPDATE\n\t" + targetTable.getName() + "\n";
					result += "INNER JOIN " + sourceTable.getName() + " ON " + sourceTable.getName() + "." + initialSourceField.getName() + "=" + targetTable.getName() + "." + initialTargetField.getName() + "\n";
					result += "SET " + targetTable.getName() + "." + targetField.getName() + "=" + sourceTable.getName() + "." + sourceField.getName() + ";\n\n";
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
	
	private static void sort (List<Field> fields) {
		int targetIdx = -1;
		
		for (int i = 0; i < fields.size(); i++) {
			if (fields.get(i).isUnique()) {
				targetIdx = i;
			}
		}
		
		if (targetIdx != -1) {
			Field temp = fields.get(targetIdx);
			fields.set(targetIdx, fields.get(0));
			fields.set(0, temp);
		}
	}
	
	private static boolean checkDuplicate (List<ItemToItemMap> maps) {
		
		List<Pair<Table, Field>> list = new ArrayList<>();
		
		for (ItemToItemMap map : maps) {
			Field targetField = castToField(map.getTargetItem());
			Table targetTable = targetField.getTable();
			Pair<Table, Field> pair = new Pair<>(targetTable, targetField);
			if (list.contains(pair)) {
				return false;
			}
			else {
				list.add(pair);
			}
		}
		
		return true;
	}
	
	private static boolean checkDataType (List<ItemToItemMap> maps) {
		DIRECTLY_CONVERTABLE_TYPES.put(DataType.TEXT, DataType.VARCHAR);
		DIRECTLY_CONVERTABLE_TYPES.put(DataType.VARCHAR, DataType.TEXT);
		for (ItemToItemMap map : maps) {
			Field targetField = castToField(map.getTargetItem());
			Field sourceField = castToField(map.getSourceItem());
			if (!sourceField.getType().equals(targetField.getType())) {
				if (!(DIRECTLY_CONVERTABLE_TYPES.containsKey(sourceField.getType()) && DIRECTLY_CONVERTABLE_TYPES.get(sourceField.getType()).equals(targetField.getType()))) {
					
					System.err.println("Hit");
					System.err.println(sourceField.getType());
					System.err.println(targetField.getType());
					System.err.println("result: " + DIRECTLY_CONVERTABLE_TYPES.get(sourceField.getType()));
					return false;
				}
			}
		}
		 
		return true;
	}
}
