package com.oracle.fccm.amllite.scripts.pipeline.mergegenerator;

import com.oracle.fccm.amllite.scripts.pipeline.util.Constants;

import java.io.*;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

public class Merge
{
  private static String driver;
  private static String url;
  private static String userName;
  private static String password;
  private static FileOutputStream fos = null;
  private static BufferedWriter bw = null;
  private static String condition;
  private static FileOutputStream fos1 = null;
  private static BufferedOutputStream bos1 = null;
  private static OutputStreamWriter writer1 = null;
  
  public static void openWriteFile(File file)
    throws IOException
  {
    fos1 = new FileOutputStream(file, true);
    bos1 = new BufferedOutputStream(fos1);
    writer1 = new OutputStreamWriter(fos1, "UTF8");
  }
  
  public static void writeFile(String message)
    throws IOException
  {
    writer1.write(message);
    writer1.write("\n");
  }
  
  public static void closeWriteFile()
    throws IOException
  {
    writer1.close();
  }
  
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    try (FileReader reader = new FileReader(Constants.CONFIG_FILE_PATH)) {
      props.load(reader);
    } catch (IOException e) {
      System.err.println("Error reading properties file: " + e.getMessage());
      throw e;
    }

//    driver = "oracle.jdbc.driver.OracleDriver";
    
    Connection conn = null;
    try
    {
        DatabaseMetaData meta = null;
      
//      Class.forName(driver);
//      String walletPath = Constants.PARENT_DIRECTORY+File.separator+"bin"+File.separator+props.getProperty("WALLET_NAME");
      conn = getDbConnection(props);
      meta = conn.getMetaData();
      String querymain = "select table_name from user_tables where table_name IN  ('"+props.getProperty("TABLE_NAME")+"') order by 1";
      Statement tablemain = conn.createStatement();
      System.out.println(querymain);
//      String version = "v0";
      //String version = "";
      ResultSet tablemainresult = tablemain.executeQuery(querymain);
      ArrayList<String> arrlist = new ArrayList<String>();
      while (tablemainresult.next()) {
          Statement statement = null;
          ResultSet resultSet = null;
          ResultSetMetaData metadata = null;
          int columnCount = 0; 
          int column = 1;
          int conditionCount = 1;
          
          StringBuffer query = new StringBuffer();
          StringBuffer insertColumns = new StringBuffer();
          StringBuffer insertValues = new StringBuffer();
          StringBuffer selectCondition = new StringBuffer();
          StringBuffer insertQuery = new StringBuffer();
          File fout = null;
	  
      String table = tablemainresult.getString("table_name");
      System.out.println("Table Name :- "+ table);
      HashMap<String, String> pColumns = new HashMap();
      SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
      
      fout = new File(Constants.OUTPUT_FOLDER + File.separator + table+ ".sql");
      openWriteFile(fout);
      
      System.out.println("Retrieving the primary keys for the table");
      Statement tableStatement = conn.createStatement();
      String query1 = "select t.column_name from user_cons_columns t where t.constraint_name in (select f.constraint_name from user_constraints f where f.CONSTRAINT_TYPE = 'P' and f.TABLE_NAME = '" + table.trim().toUpperCase() + "')";
      ResultSet tableResultSet = tableStatement.executeQuery(query1);
      HashMap<String, String> keys = new HashMap();
      while (tableResultSet.next())
      {
        keys.put(tableResultSet.getString(1), tableResultSet.getString(1));

      }
      String[] primaryKeys = new String[keys.values().size()];
      int x = 0;
      for (String key : keys.values())
      {
        primaryKeys[x] = key;
        x++;
      }
      tableResultSet.close();
      tableStatement.close();
      statement = conn.createStatement();
      condition = " "+props.getProperty("WHERE_CONDITION")+" ";
      if ((condition != null) && (!"".equals(condition))) {
        System.out.println("Query executing:SELECT * FROM " + table + " where " + condition );
        resultSet = statement.executeQuery("SELECT * FROM " + table + " where " + condition);
      }
      else
      {
         resultSet = statement.executeQuery("SELECT * FROM " + table);
      }
      metadata = resultSet.getMetaData();
      columnCount = metadata.getColumnCount();
      ArrayList<String> columns = new ArrayList();
      Object columnTypes = new HashMap();
      for (int i = 1; i <= columnCount; i++)
      {
        String columnName = metadata.getColumnName(i);
        ((HashMap)columnTypes).put(columnName, metadata.getColumnTypeName(i));
        columns.add(columnName);
      }
      int length = primaryKeys.length;
      for (int columnName2 = 0; columnName2 < length; columnName2++)
      {
        String key = primaryKeys[columnName2];
        pColumns.put(key, key);
      }
      boolean valuePresent = false;
      for (String columnName3 : columns) {
        if (pColumns.get(columnName3) == null)
        {
          valuePresent = true;
          break;
        }
      }
      Integer tablecount = 0;
      while (resultSet.next())
      {
    	  tablecount++;
        Object columnName1;
        if (!valuePresent)
        {
          conditionCount = 1;
          boolean dateField = false;
          insertQuery.append(" INTO " + table + " (");
          selectCondition.append("SELECT COUNT(*) FROM " + table + " where ");
          column = 1;
          for (String columnName3 : columns) {
            if (column == 1)
            {
              column = 2;
              insertQuery.append(columnName3);
            }
            else
            {
              insertQuery.append("," + columnName3);
            }
          }
          insertQuery.append(" ) VALUES (");
          column = 1;
          for (Iterator localIterator4 = columns.iterator(); localIterator4.hasNext();)
          {
            columnName1 = (String)localIterator4.next();
            
            String value = "";
            String type = (String)((HashMap)columnTypes).get(columnName1);
            if (type.contains("DATE"))
            {
              if (resultSet.getDate((String)columnName1) != null)
              {
                dateField = true;
                value = "to_date('" + format.format(resultSet.getDate((String)columnName1)) + "' , 'dd-mm-yyyy') ";
              }
            }
            else if (type.contains("TIMESTAMP"))
            {
              if (resultSet.getDate((String)columnName1) != null)
              {
                dateField = true;
                value = "to_date('" + format.format(resultSet.getDate((String)columnName1)) + "' , 'dd-mm-yyyy') ";
              }
            }
            else if (resultSet.getString((String)columnName1) != null) {
              value = resultSet.getString((String)columnName1).replaceAll("'", "''");
            }
            if (pColumns.get(columnName1) != null) {
              if (conditionCount == 1)
              {
                if (dateField) {
                  selectCondition.append(columnName1 + "=" + value + " ");
                } else {
                  selectCondition.append(columnName1 + "='" + value + "' ");
                }
                conditionCount = 2;
              }
              else
              {
                selectCondition.append(" AND " + (String)columnName1 + "='" + value + "' ");
              }
            }
            if (column == 1)
            {
              if (dateField) {
                insertQuery.append(value + " ");
              } else {
                insertQuery.append("'" + value + "' ");
              }
              column = 2;
            }
            else if (dateField)
            {
              insertQuery.append(", " + value + " ");
            }
            else
            {
              insertQuery.append(", '" + value + "' ");
            }
          }
          insertQuery.append(" )");
          
          query.append("INSERT WHEN ((" + selectCondition + ")=0) THEN " + insertQuery + selectCondition + "\n/");
        }
        else
        {
          column = 1;
          
          
          query.append("MERGE INTO " + table.toUpperCase() + " T USING ( \n SELECT ");
          for (columnName1 = columns.iterator(); ((Iterator)columnName1).hasNext();)
          {
            String columnName21 = (String)((Iterator)columnName1).next();
            String value = "";
//            System.out.println();
            boolean dateField = false;
            String type = (String)((HashMap)columnTypes).get(columnName21);
			if (type.equalsIgnoreCase("CLOB")) {
				dateField = true;
				value = handleClob(resultSet,columnName21);
			}
            else if (type.contains("DATE"))
            {
              if (resultSet.getDate(columnName21) != null)
              {
                dateField = true;
                value = "to_date('" + format.format(resultSet.getDate(columnName21)) + "' , 'dd-mm-yyyy') ";
              }
            }
            else if (type.contains("TIMESTAMP"))
            {
              if (resultSet.getDate(columnName21) != null)
              {
                dateField = true;
                value = "to_date('" + format.format(resultSet.getDate(columnName21)) + "' , 'dd-mm-yyyy') ";
              }
            }
            else if (resultSet.getString(columnName21) != null) {
              value = resultSet.getString(columnName21).replaceAll("'", "''");
            }
            if (column == 1)
            {
              if (dateField) {
                query.append(value + " " + columnName21);
              } else {
                query.append("'" + value + "' " + columnName21);
              }
              insertColumns.append(columnName21);
              insertValues.append("S." + columnName21);
              column = 2;
            }
            else
            {
              if (dateField) {
                query.append(", " + value + " " + columnName21);
              } else {
                query.append(", '" + value + "' " + columnName21);
              }
              insertColumns.append("," + columnName21);
              insertValues.append(",S." + columnName21);
            }
          }
          query.append(" FROM DUAL) S \n ON ( ");
          
          column = 1;
          for (int columnName2 = 0; columnName2 < length; columnName2++)
          {
            String key = primaryKeys[columnName2];
            if (column == 1)
            {
              query.append("T." + (String)key + " = S." + (String)key);
              column = 2;
            }
            else
            {
              query.append(" AND T." + (String)key + " = S." + (String)key);
            }
          }
          query.append(" )\n WHEN MATCHED THEN UPDATE SET ");
          
          column = 1;
          for (Object key = columns.iterator(); ((Iterator)key).hasNext();)
          {
            String columnName3 = (String)((Iterator)key).next();
            if (pColumns.get(columnName3) == null) {
              if (column == 1)
              {
                query.append("T." + columnName3 + " = S." + columnName3);
                column = 2;
              }
              else
              {
                query.append(", T." + columnName3 + " = S." + columnName3);
              }
            }
          }
          query.append(" \n WHEN NOT MATCHED THEN INSERT \n (" + insertColumns.toString() + ")\n VALUES \n (" + insertValues.toString() + ")\n/");
          if (tablecount % 1000 == 0) {
          query.append("\n commit\n/");
		}
        }        
        writeFile(query.toString());
        query = new StringBuffer();
        insertColumns = new StringBuffer();
        insertValues = new StringBuffer();
        selectCondition = new StringBuffer();
        insertQuery = new StringBuffer();
      }
    //  query.append("commit");
      writeFile("commit\n/");
      resultSet.close();
      statement.close();
      closeWriteFile();
      arrlist.add(table);      
      }
      
      //CreateChangeLogFile  changelog = new CreateChangeLogFile();
      //changelog.changelogfile(arrlist,version);
      conn.close();
      System.out.println("Merge Script created successfully!!!");
       
    }
    catch (ClassNotFoundException e)
    {
      System.out.println("Could not find the database driver " + e.getMessage());
      e.printStackTrace();
    }
    catch (SQLException e)
    {
      System.out.println("Could not connect to the database " + e.getMessage());
      e.printStackTrace();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }
  public static Connection getDbConnection(Properties props) throws Exception {
    try (FileReader reader = new FileReader(Constants.CONFIG_FILE_PATH)) {
      props.load(reader);
    } catch (IOException e) {
      System.err.println("Error reading properties file: " + e.getMessage());
      throw e;
    }

    String jdbcUrl = props.getProperty("jdbcurl");
    String jdbcDriver = props.getProperty("jdbcdriver");
    String username = props.getProperty("username");
    String password = props.getProperty("password");
    String walletname = props.getProperty("walletName");
    String tnsAdminPath = Constants.PARENT_DIRECTORY+File.separator+"bin"+File.separator+walletname;

    Properties properties = new Properties();
    properties.setProperty("user", username);
    properties.setProperty("password", password);
    properties.setProperty("oracle.net.tns_admin", tnsAdminPath);
    Class.forName(jdbcDriver);
    Connection connection = DriverManager.getConnection(jdbcUrl,properties);
    System.out.println("Connection established successfully!");
    return connection;
  }

	private static String handleClob(ResultSet resultSet, String columnName1) {

		String value="";
		Clob valClob = null;
		try {
			valClob = resultSet.getClob(columnName1);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		if (valClob != null) {
			StringBuffer sb = new StringBuffer();
			StringBuffer sb1 = new StringBuffer(
					"TO_CLOB(q'@");
			try {
				Reader reader = valClob
						.getCharacterStream();
				BufferedReader br = new BufferedReader(
						reader);
				int readBit;
				int countBit = 0;
				while (-1 != (readBit = br.read())) {
					sb1.append((char) readBit);
						if (countBit == 3000) {
							countBit = 0;
							sb1.append("@') || ");
							sb.append(sb1);
							sb1 = new StringBuffer(
									"TO_CLOB(q'@");
					}
					countBit++;
				}
				
				if(!sb1.toString().equals("TO_CLOB(q'@")){
					sb.append(sb1);
					sb.append("@')");
					value=sb.toString();
				}else{
					value=sb.toString().substring(0,sb.toString().length()-3); 
				}
					
				br.close();

			} catch (Exception e) {
				e.printStackTrace();

			}
		} else {
			value = null;
		}
		return value;
	
	}
 
  
  
  
  public static String[] getPrimaryKeys(String tableName, DatabaseMetaData meta)
    throws SQLException
  {
    ResultSet rs = meta.getPrimaryKeys(null, userName.toUpperCase(), tableName.toUpperCase());
    String tables = null;
    boolean more = false;
    while (rs.next()) {
      if (!more)
      {
        tables = rs.getString("COLUMN_NAME");
        more = true;
      }
      else
      {
        tables = tables + "," + rs.getString("COLUMN_NAME");
      }
    }
    return tables.split(",");
  }
}

