/*******************************************************************************
 * Copyright 2016 Observational Health Data Sciences and Informatics
 * 
 * This file is part of WhiteRabbit
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ohdsi.rabbitInAHat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.ohdsi.rabbitInAHat.dataModel.Database;
import org.ohdsi.rabbitInAHat.dataModel.ETL;
import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.WriteTextFile;

public class ETLTestFrameWorkGenerator {

	public static String[]		keywords	= new String[] { "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "BACKUP", "BEGIN", "BETWEEN",
		"BREAK", "BROWSE", "BULK", "BY", "CASCADE", "CASE", "CHECK", "CHECKPOINT", "CLOSE", "CLUSTERED", "COALESCE", "COLLATE", "COLUMN", "COMMIT",
		"COMPUTE", "CONSTRAINT", "CONTAINS", "CONTAINSTABLE", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME",
		"CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DBCC", "DEALLOCATE", "DECLARE", "DEFAULT", "DELETE", "DENY", "DESC", "DISK",
		"DISTINCT", "DISTRIBUTED", "DOUBLE", "DROP", "DUMP", "ELSE", "END", "ERRLVL", "ESCAPE", "EXCEPT", "EXEC", "EXECUTE", "EXISTS", "EXIT", "EXTERNAL",
		"FETCH", "FILE", "FILLFACTOR", "FOR", "FOREIGN", "FREETEXT", "FREETEXTTABLE", "FROM", "FULL", "FUNCTION", "GOTO", "GRANT", "GROUP", "HAVING",
		"HOLDLOCK", "IDENTITY", "IDENTITY_INSERT", "IDENTITYCOL", "IF", "IN", "INDEX", "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN", "KEY", "KILL",
		"LEFT", "LIKE", "LINENO", "LOAD", "MERGE", "NATIONAL", "NOCHECK", "NONCLUSTERED", "NOT", "NULL", "NULLIF", "OF", "OFF", "OFFSETS", "ON", "OPEN",
		"OPENDATASOURCE", "OPENQUERY", "OPENROWSET", "OPENXML", "OPTION", "OR", "ORDER", "OUTER", "OVER", "PERCENT", "PIVOT", "PLAN", "PRECISION",
		"PRIMARY", "PRINT", "PROC", "PROCEDURE", "PUBLIC", "RAISERROR", "READ", "READTEXT", "RECONFIGURE", "REFERENCES", "REPLICATION", "RESTORE",
		"RESTRICT", "RETURN", "REVERT", "REVOKE", "RIGHT", "ROLLBACK", "ROWCOUNT", "ROWGUIDCOL", "RULE", "SAVE", "SCHEMA", "SECURITYAUDIT", "SELECT",
		"SEMANTICKEYPHRASETABLE", "SEMANTICSIMILARITYDETAILSTABLE", "SEMANTICSIMILARITYTABLE", "SESSION_USER", "SET", "SETUSER", "SHUTDOWN", "SOME",
		"STATISTICS", "SYSTEM_USER", "TABLE", "TABLESAMPLE", "TEXTSIZE", "THEN", "TO", "TOP", "TRAN", "TRANSACTION", "TRIGGER", "TRUNCATE", "TRY_CONVERT",
		"TSEQUAL", "UNION", "UNIQUE", "UNPIVOT", "UPDATE", "UPDATETEXT", "USE", "USER", "VALUES", "VARYING", "VIEW", "WAITFOR", "WHEN", "WHERE", "WHILE",
		"WITH", "WITHIN GROUP", "WRITETEXT" };

	private static Set<String>	keywordSet;
	private static int DEFAULT = 0;
	private static int NEGATE = 1;
	private static int COUNT = 2;
			

	public static void generate(ETL etl, String filename) {
		keywordSet = new HashSet<String>();
		for (String keyword : keywords)
			keywordSet.add(keyword);
		List<String> r = generateRScript(etl);
		WriteTextFile out = new WriteTextFile(filename);
		for (String line : r)
			out.writeln(line);
		out.close();
	}

	private static String convertToRName(String name) {
		name = name.replaceAll(" ", "_").replaceAll("-", "_");
		return name;
	}
	
	private static List<String> generateRScript(ETL etl) {
		List<String> r = new ArrayList<String>();
		createFactoryFunction(r, etl);
		return r;
	}

	private static void createDeclareTestFunction(List<String> r) {
		r.add("  self$declareTest <- function(id, description) {");
		r.add("    self$testId <- id;");
		r.add("    self$testDescription <- description;");
		r.add("    sql <- c(\"\", paste0(\"-- \", id, \": \", description))");
		r.add("    self$insertSql <- c(self$insertSql, sql);");
		r.add("    self$testSql <- c(self$testSql, sql);");
		r.add("  }");
		r.add("");
	}

	private static void createExpectFunctions(List<String> r, int type, Database database) {
		for (Table table : database.getTables()) {
			StringBuilder line = new StringBuilder();
			String rTableName = convertToRName(table.getName());
			String sqlTableName = convertToSqlName(table.getName());
			List<String> argDefs = new ArrayList<String>();
			List<String> testDefs = new ArrayList<String>();
			for (Field field : table.getFields()) {
				String rFieldName = convertToRName(field.getName());
				String sqlFieldName = convertToSqlName(field.getName());
				argDefs.add(rFieldName);
				testDefs.add("    if (!missing(" + rFieldName + ")) {");
				testDefs.add("      if (first) {");
				testDefs.add("        first <- FALSE;");
				testDefs.add("      } else {");
				testDefs.add("        statement <- paste0(statement, \" AND\");");
				testDefs.add("      }");
				testDefs.add("      if (is.null(" + rFieldName + ")) {");
				testDefs.add("        statement <- paste0(statement, \" " + sqlFieldName + " IS NULL\");");
				testDefs.add("      } else {");
				testDefs.add("        statement <- paste0(statement, \" " + sqlFieldName + " = '\", " + rFieldName + ",\"'\");");
				testDefs.add("      }");
				testDefs.add("    }");
				testDefs.add("");
			}

			if (type == DEFAULT) 
				line.append("  self$expect_" + rTableName + " <- function(");
			else if (type == NEGATE)
				line.append("  self$expect_no_" + rTableName + " <- function(");
			else
				line.append("  self$expect_count_" + rTableName + " <- function(rowCount, ");

			line.append(StringUtilities.join(argDefs, ", "));
			line.append(") {");
			r.add(line.toString());

			line = new StringBuilder();
			line.append("    statement <- paste0(\"INSERT INTO test_results SELECT ");
			line.append("\", get(\"testId\", envir = globalenv()), \" AS id, ");
			line.append("'\", get(\"testDescription\", envir = globalenv()), \"' AS description, ");
			line.append("'Expect " + table.getName() + "' AS test, ");
			line.append("CASE WHEN(SELECT COUNT(*) FROM " + sqlTableName + " WHERE\")");
			r.add(line.toString());

			r.add("    first <- TRUE");

			r.addAll(testDefs);

			if (type == DEFAULT) 
				r.add("    statement <- paste0(statement, \") = 0 THEN 'FAIL' ELSE 'PASS' END AS status;\")");
			else if (type == NEGATE)
				r.add("    statement <- paste0(statement, \") != 0 THEN 'FAIL' ELSE 'PASS' END AS status;\")");
			else
				r.add("    statement <- paste0(statement, \") != \",rowCount ,\" THEN 'FAIL' ELSE 'PASS' END AS status;\")");

			r.add("    self$testSql <- c(self$testSql, statement);");
			r.add("  }");
			r.add("");
		}
	}

	private static String convertToSqlName(String name) {
		if (name.contains(" ") || name.contains(".") || keywordSet.contains(name.toUpperCase()))
			name = "[" + name + "]";
		return name;
	}


	private static void createAddFunctions(List<String> r, Database database) {
		for (Table table : database.getTables()) {
			StringBuilder line = new StringBuilder();
			String rTableName = convertToRName(table.getName());
			String sqlTableName = convertToSqlName(table.getName());
			List<String> argDefs = new ArrayList<String>();
			List<String> insertLines = new ArrayList<String>();
			for (Field field : table.getFields()) {
				String rFieldName = field.getName().replaceAll(" ", "_").replaceAll("-", "_");
				String sqlFieldName = convertToSqlName(field.getName());
				argDefs.add(rFieldName);
				insertLines.add("    if (missing(" + rFieldName + ")) {");
				insertLines.add("      " + rFieldName + " <- defaults$" + rFieldName);
				insertLines.add("    }");
				insertLines.add("    if (!is.null(" + rFieldName + ")) {");
				insertLines.add("      insertFields <- c(insertFields, \"" + sqlFieldName + "\")");
				insertLines.add("      insertValues <- c(insertValues, " + rFieldName + ")");
				insertLines.add("    }");
				insertLines.add("");
			}

			line.append("  self$add_" + rTableName + " <- function(");
			line.append(StringUtilities.join(argDefs, ", "));
			line.append(") {");
			r.add(line.toString());
			r.add("    defaults <- self$defaultValues$" + rTableName + ";");
			r.add("    insertFields <- c();");
			r.add("    insertValues <- c();");
			r.addAll(insertLines);

			line = new StringBuilder();
			line.append("    statement <- paste0(\"INSERT INTO " + sqlTableName + " (\", ");
			line.append("paste(insertFields, collapse = \", \"), ");
			line.append("\") VALUES ('\", ");
			line.append("paste(insertValues, collapse = \"', '\"), ");
			line.append("\"')\");");
			r.add(line.toString());

			r.add("    self$insertSql <- c(self$insertSql, statement);");
			r.add("  }");
			r.add("");
		}
	}
	
	private static void createSetDefaultFunctions(List<String> r, Database database) {
		for (Table table : database.getTables()) {
			StringBuilder line = new StringBuilder();
			String rTableName = convertToRName(table.getName());
			List<String> argDefs = new ArrayList<String>();
			List<String> insertLines = new ArrayList<String>();
			for (Field field : table.getFields()) {
				String rFieldName = field.getName().replaceAll(" ", "_").replaceAll("-", "_");
				argDefs.add(rFieldName);
				insertLines.add("    if (!missing(" + rFieldName + ")) {");
				insertLines.add("      defaults$" + rFieldName + " <- " + rFieldName + ";");
				insertLines.add("    }");
			}

			line.append("  self$set_defaults_" + rTableName + " <- function(");
			line.append(StringUtilities.join(argDefs, ", "));
			line.append(") {");
			r.add(line.toString());
			r.add("    defaults <- self$defaultValues$" + rTableName + ";");
			r.addAll(insertLines);
			r.add("    self$defaultValues$" + rTableName + " <- defaults;");
			r.add("  }");
			r.add("");
		}
	}
	
	private static void createGetDefaultFunctions(List<String> r, Database database) {
		for (Table table : database.getTables()) {
			String rTableName = convertToRName(table.getName());
			r.add("  self$get_defaults_" + rTableName + " <- function() {");
			r.add("    defaults <- self$defaultValues$" + rTableName + ";");
			r.add("    return(defaults);");
			r.add("  }");
			r.add("");
		}
	}
	
	private static void createFactoryFunction(List<String> r, ETL etl) {
		Database database = etl.getSourceDatabase();
		
		r.add("#' Create test framework object");
		r.add("#'");
		r.add("#' @details");
		r.add("#' Contains functions to condunct testing.");
		r.add("#'");
		r.add("#' @return");
		r.add("#' a framework instance");
		r.add("#'");
		r.add("#' @export");
		r.add("createTestFramework <- function() {");
		r.add("  self <- new.env(parent = emptyenv());");
		for (Table table : database.getTables()) {
			String sqlTableName = convertToSqlName(table.getName());
			r.add("  self$insertSql <- c(self$insertSql, \"TRUNCATE TABLE " + sqlTableName + ";\");");
		}

		r.add("  self$testSql <- c();");
		r.add("  self$testSql <- c(self$testSql, \"IF OBJECT_ID('@testSchema.test_results', 'U') IS NOT NULL\");");
		r.add("  self$testSql <- c(self$testSql, \"  DROP TABLE @testSchema.test_results;\");");
		r.add("  self$testSql <- c(self$testSql, \"\");");
		r.add("  self$testSql <- c(self$testSql, \"CREATE TABLE @testSchema.test_results (id INT, description VARCHAR(512), test VARCHAR(256), status VARCHAR(5))\");");
		r.add("  self$testSql <- c(self$testSql, \"\");");
		r.add("  self$testId <- 1;");
		r.add("  self$testDescription <- \"\";");
		r.add("");
		r.add("  self$defaultValues <- list()");
		for (Table table : database.getTables()) {
			r.add("");
			String rTableName = convertToRName(table.getName());
			r.add("  defaults <- list();");
			for (Field field : table.getFields()) {
				String rFieldName = field.getName().replaceAll(" ", "_").replaceAll("-", "_");
				String defaultValue;
				if (field.getValueCounts().length == 0)
					defaultValue = "";
				else
					defaultValue = field.getValueCounts()[0][0];
				if (!defaultValue.equals(""))
					r.add("  defaults$" + rFieldName + " <- \"" + defaultValue + "\";");
			}
			r.add("  self$defaultValues$" + rTableName + " <- defaults;");
		}
		r.add("");
		createDeclareTestFunction(r);
		r.add("");
		createSetDefaultFunctions(r, etl.getSourceDatabase());
		r.add("");
		createGetDefaultFunctions(r, etl.getSourceDatabase());
		r.add("");
		createAddFunctions(r, etl.getSourceDatabase());
		r.add("");
		createExpectFunctions(r, DEFAULT, etl.getTargetDatabase());
		r.add("");
		createExpectFunctions(r, NEGATE, etl.getTargetDatabase());
		r.add("");
		createExpectFunctions(r, COUNT, etl.getTargetDatabase());
		r.add("  return (self);");
		r.add("}");
		r.add("");
	}	
}
