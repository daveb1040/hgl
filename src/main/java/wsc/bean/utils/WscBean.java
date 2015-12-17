package wsc.bean.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.Date;
import java.util.Hashtable;

import javax.naming.InitialContext;
import javax.sql.DataSource;


import weidenhammer.com.utils.ArrayHelper;
import weidenhammer.com.utils.DateHelper;
import weidenhammer.com.utils.Log;
import weidenhammer.com.utils.ReflectionHelper;
import weidenhammer.com.utils.StringHelper;


public abstract class WscBean 
{
	private static Hashtable nextUniqueIds = new Hashtable();
	private static Hashtable tableNames = new Hashtable();
	
	private WscDbContext wscDbContext;
	private String tableName = null;
	private static Hashtable allFieldNames = new Hashtable();
	private static Hashtable allKeyFields = new Hashtable();
	private static boolean toLowerCase = false;

	
	public abstract String getIdFieldName();

	protected WscBean()
	{}

	public WscBean(WscDbContext wscDbContext_)
	{
		wscDbContext = wscDbContext_;
	}

	private void sideBar(ResultSet rs) throws Exception
	{
		ResultSetMetaData rsmd = rs.getMetaData();
		Log.log(10, "**********   The tablename := " + this.getTableName());
		for(int i = 1; i <= rsmd.getColumnCount(); i++)
		{
			String columnName = rsmd.getColumnName(i);
			Log.log(10, "  *** column name := " + columnName + " The tablename := " + rsmd.getTableName(i));
		}
	}
	
	
	public WscBean(WscDbContext wscDbContext, ResultSet rs) throws Exception
	{
		//  This was added because we need to invoke the getTableName method, and we need to create an 
		//  instance of the bean to do that.
		if (wscDbContext == null && rs == null)
			return;
		ResultSetMetaData rsmd = rs.getMetaData();
		setWscDbContext(wscDbContext);
		for(int i = 1; i <= rsmd.getColumnCount(); i++)
		{
			String columnName = rsmd.getColumnName(i);
			String setMethodName = "set" + StringHelper.upperCase(columnName);
			Method setMethod = ReflectionHelper.getMethod(this, setMethodName);
			Object[] parameters = new Object[1];
			
Log.log(10, " #DB column := " + rsmd.getColumnName(i) + "  type := " + rsmd.getColumnType(i) + "setMethodName : " + setMethodName);			
			switch(rsmd.getColumnType(i))
			{
				case Types.BIGINT:
					parameters[0] = new Integer(rs.getInt(i));
					break;
				case Types.BIT:
					parameters[0] = new Boolean(rs.getInt(i) == 1); // 1 = true; 0 = false
					break;
				case Types.BOOLEAN:
					parameters[0] = new Boolean(rs.getBoolean(i));
					break;
				case Types.CHAR:
					parameters[0] = new String(rs.getString(i));
					break;
				case Types.DATE:
					parameters[0] = rs.getDate(i);
					break;
				case Types.DECIMAL:
					parameters[0] = new Double(rs.getDouble(i));
					break;
				case Types.DOUBLE:
					parameters[0] = new Double(rs.getDouble(i));
					break;
				case Types.FLOAT:
					parameters[0] = new Float(rs.getFloat(i));
					break;
				case Types.INTEGER:
					parameters[0] = new Integer(rs.getInt(i));
					break;
				case Types.LONGVARCHAR:
					parameters[0] = new String(rs.getString(i));
					break;
				case Types.REAL:
					parameters[0] = new Double(rs.getDouble(i));
					break;
				case Types.SMALLINT:
					parameters[0] = new Boolean(rs.getBoolean(i));
//					parameters[0] = new Integer(rs.getInt(i)); 
					break;
				case Types.TIMESTAMP:
					parameters[0] = DateHelper.getDate(rs.getTimestamp(i));
					break;
				case Types.TINYINT:
					parameters[0] = new Integer(rs.getInt(i));
					break;
				case Types.VARCHAR:
					Log.log(10, "rs.getString(i) := " + rs.getString(i));
					parameters[0] = new String(rs.getString(i)); // 
					break;
			}
			setMethod.invoke(this, parameters);						
		}
	}
	
	private static synchronized long getNextUniqueIdSynchronized(Class childClass, String tableName, String idFieldName, WscDbContext wscDbContext)
	{
		long oldNextUniqueId = 0;
		Long newNextUniqueId = (Long)nextUniqueIds.get(childClass);
		//  if id == null, then go get the id from the database
		if (newNextUniqueId == null)
		{
			try
			{			
				Statement statement = wscDbContext.getConnection().createStatement();			
				String sqlString = "select MAX(" + idFieldName + ") from " + tableName + ";";
				//System.out.println("sql := " + sqlString);
				ResultSet rs = statement.executeQuery(sqlString);
				if (rs != null && rs.next())
					oldNextUniqueId = rs.getLong(1) + 1;
				else
					oldNextUniqueId = 1;
				
				newNextUniqueId = new Long(oldNextUniqueId + 1);
				nextUniqueIds.put(childClass, newNextUniqueId);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		// else, we have one stored already, so increment, and replace
		else
		{
			oldNextUniqueId = newNextUniqueId.longValue();
			nextUniqueIds.remove(childClass);
			nextUniqueIds.put(childClass, new Long(oldNextUniqueId + 1));
		}
		//System.out.println("oldNextUniqid := " + oldNextUniqueId);
		return oldNextUniqueId;
			
	}
	
	public long getNextUniqueId()
	{		
		return getNextUniqueIdSynchronized(this.getClass(), getTableName(), getIdFieldName(), wscDbContext);
	}
	
	public String getTableName()
	{
		if (tableName == null)
		{
//			String className = this.getClass().getName();
			String className = this.getClass().getName().toLowerCase();
			int index = className.lastIndexOf(".");
			tableName =  className.substring(index + 1);
		}
		return tableName;
	}
	
	
	public static QueryCriteria getQueryCriteria(WscDbContext wscDbContext, Class beanClass) throws Exception
	{
		return new QueryCriteria(wscDbContext, getFieldNames(wscDbContext, beanClass));
	}
	
	
	protected void setWscDbContext(WscDbContext wscDbContext_) {
		this.wscDbContext = wscDbContext_;
	}

	protected WscDbContext getWscDbContext() {
		return wscDbContext;
	}
	
	public static DatabaseField[] getFieldNames(WscDbContext wscDbContext, Class beanClass) throws Exception
	{
		Log.log(11, "getFieldNames, beanClass.getName := " + beanClass.getName());
		Object fieldNames = allFieldNames.get(getTableNameFromClass(beanClass));
		if (fieldNames == null)
		{
			Log.log(11, "fieldNames == null");
			fieldNames = createFieldNames(wscDbContext, beanClass);
			allFieldNames.put(beanClass, fieldNames);
			
		}
		else
			Log.log(11, "fieldNames != null");
			
		return (DatabaseField[]) fieldNames;
	}
	
	public static DatabaseField[] getKeyFields(WscDbContext wscDbContext, Class beanClass) throws Exception
	{
		Object keyFields = allKeyFields.get(beanClass);
		if (keyFields == null)
		{
			keyFields = createKeyFields(wscDbContext, beanClass);
			allKeyFields.put(beanClass, keyFields);
			
		}
		return (DatabaseField[]) keyFields;
	}
	
	public static DatabaseField[] createFieldNames(WscDbContext wscDbContext, Class beanClass) throws Exception
	{
		Log.log(11, "CreateFieldNames, beanClass.getName := " + beanClass.getName());
		DatabaseMetaData md = wscDbContext.getConnection().getMetaData();
		String tableName = getTableNameFromClass(beanClass);
		String schemaName = null;
		//System.out.println("bean class simpla name := '" + tableName + "'");		
		ResultSet columns = md.getColumns(null, schemaName, tableName, null);
		
		int columnCount = 0;
		while(columns.next())
			columnCount++;

		/********************************************************************************************
		 * Commented by David Betancourt:
		 * I will be the first to tell you that this is some nasty code, but time constraints 
		 * made this happen.  The issue is with db2.  When we run this application any place other 
		 * than the iseries, the preceding code workds fine.  But when we deploy to iseries, we need 
		 * to specify the schema name.  Also, we need to capitalize the table name.  We will use that
		 * as our flag to determine whether or not they are using db2 or a good database...
		 * If the column count is 0, we know then are using db2.  At this moment, JAVAFRMWRK is the 
		 * schema that must be specified.
		 *******************************************************************************************/
		if (columnCount == 0)
		{
//			System.out.println("db2 database");
			schemaName = wscDbContext.getSchemaName();
			tableName = tableName.toUpperCase();
			ResultSet db2Columns = md.getColumns(null, schemaName, tableName, null);
			while(db2Columns.next())
				columnCount++;
		}
		
		if (columnCount == 0)
		{
//			System.out.println("not db2 database, let's try doing lowercase");
			schemaName = wscDbContext.getSchemaName();
			tableName = tableName.toLowerCase();
			columns = md.getColumns(null, schemaName, tableName, null);
			while(columns.next())
				columnCount++;
			toLowerCase = true;
		}
		
//		System.out.println("Column count for table = " + columnCount);
		DatabaseField[] databaseFields = new DatabaseField[columnCount];
		
		columns = md.getColumns(null, schemaName, tableName, null);
		
		while(columns.next())
		{
			String name = columns.getString("COLUMN_NAME");
			Field fieldIndex = beanClass.getField(name.toUpperCase());
			Log.log(9, "beanClass := " + beanClass.getName()  + ", name := " + name + ", " + columns.getInt("DATA_TYPE"));
			databaseFields[fieldIndex.getInt(null)] = new DatabaseField(name, getTableNameFromClass(beanClass), 
																  columns.getInt("DATA_TYPE"), columns.getInt("COLUMN_SIZE"));
		}
/*		
		for(int i = 0; i < databaseFields.length; i++)
			Log.log(10, " databaseField[ " + i + " ] := " + databaseFields[i]);
		*/
		return databaseFields;
	}
	
	public static DatabaseField[] createKeyFields(WscDbContext wscDbContext, Class beanClass) throws Exception
	{
		String schemaName = null;
		String tableName = getTableNameFromClass(beanClass);
		DatabaseMetaData md = wscDbContext.getConnection().getMetaData();
		ResultSet keys = md.getPrimaryKeys(null, schemaName, tableName);
		int keyCount = 0;
		while(keys.next())
			keyCount++;
		
		/********************************************************************************************
		 * Commented by David Betancourt:
		 * Same bad code as commented above
		 *******************************************************************************************/
		if (keyCount == 0)
		{
			//System.out.println(" keys db2 database");
			schemaName = wscDbContext.getSchemaName();
			tableName = tableName.toUpperCase();
			ResultSet db2Columns = md.getPrimaryKeys(null, schemaName, tableName);
			while(db2Columns.next())
				keyCount++;
		}
		//System.out.println("Key Table Name := " + tableName  + ", schemaName := " + schemaName + ",  keyCount := " + keyCount);
		
		DatabaseField[] databaseFields = new DatabaseField[keyCount];
		
		keys = md.getPrimaryKeys(null, schemaName, tableName);
		for(int i = 0; keys.next(); i++)
		{
			String columnName = keys.getString("COLUMN_NAME");
//			Field fieldIndex = bean.getClass().getField(columnName.toUpperCase());
			Log.log(9, " in for loop Key Table Name := " + tableName  + ", name := " + columnName);
			ResultSet column = md.getColumns(null, schemaName, tableName, columnName);
			column.next();  // There should be 1 matching column in the result set. 
			databaseFields[i] = new DatabaseField(columnName, 
					                              extractSimpleName(beanClass), 
												  column.getInt("DATA_TYPE"),
												  column.getInt("COLUMN_SIZE"));
		}
/*		
		for(int i = 0; i < databaseFields.length; i++)
			Log.log(10, " key databaseField[ " + i + " ] := " + databaseFields[i]);
		*/
		return databaseFields;
	}
	
	
	public void insert() throws Exception
	{
		String sqlString = "insert into " + getTableName() + "(";
		String valuesString = ") values("; 
		DatabaseField[] fields = getFieldNames(wscDbContext, this.getClass());
		for(int i = 0; i < fields.length; i++)
		{
			if (i > 0)
			{
				sqlString += ", ";
				valuesString += ", ";
			}
			sqlString += fields[i].getFieldName();
			valuesString += "?";
		}
		
		sqlString += valuesString + ")";
				
//		Log.log(10, "sqlString := " + sqlString);
		PreparedStatement statement = this.wscDbContext.getConnection().prepareStatement(sqlString);
		
		setPreparedStatementVariables(statement, fields);
        statement.execute();
	}
	
	
	 public void update() throws Exception
	 {
			String sqlString = "update " + getTableName() + " set ";
			DatabaseField[] fields = getFieldNames(wscDbContext, this.getClass());
			for(int i = 0; i < fields.length; i++)
			{
				if (i > 0)
					sqlString += ", ";
				sqlString += fields[i].getFieldName() + " = ?";
			}

			sqlString += " where ";
			DatabaseField[] keys = getKeyFields(wscDbContext, this.getClass());
			for(int i = 0; i < keys.length; i++)
			{
				if (i > 0)
					sqlString += ", ";
				sqlString += keys[i].getFieldName() + " = ?";
			}

					
			Log.log(5, "update sqlString := " + sqlString);
			PreparedStatement statement = this.wscDbContext.getConnection().prepareStatement(sqlString);
			
			DatabaseField[] allFields = (DatabaseField[])ArrayHelper.concatArrays(fields, keys);
			
//			for(int i = 0; i < allFields.length; i++)
//				Log.log(10, "update Fields := " + allFields[i].toString());
			setPreparedStatementVariables(statement, allFields);
	        statement.execute();
	 }
	
	
	 public void delete() throws Exception
	 {
			String sqlString = "delete from " + getTableName() + " where ";
			DatabaseField[] keys = getKeyFields(wscDbContext, this.getClass());
			
			for(int i = 0; i < keys.length; i++)
			{
				if (i > 0)
					sqlString += ", ";
				sqlString += keys[i].getFieldName() + " = ?";
			}
					
			PreparedStatement statement = this.wscDbContext.getConnection().prepareStatement(sqlString);
			
			for(int i = 0; i < keys.length; i++)
				Log.log(10, "delete keys := " + keys[i].toString());
			setPreparedStatementVariables(statement, keys);
	        statement.execute();
	 }
	
	
	public void setPreparedStatementVariables(PreparedStatement statement, DatabaseField[] fields) throws Exception
	{
		for(int j = 0; j < fields.length; j++)
		{
			//System.out.println("  fieldn name  := " + fields[j].getFieldName());
			Method getMethod = ReflectionHelper.getMethod(this, "get" + StringHelper.upperCase(fields[j].getFieldName()));
			Object result = getMethod.invoke(this, null);						
			int i = j + 1;
			
			switch(fields[j].getFieldType())
			{
				case Types.BIGINT:
					statement.setLong(i, ((Long)result).longValue());
					break;
				case Types.BIT:
					statement.setInt(i, ((Boolean)result).booleanValue()?1: 0);
					break;
				case Types.CHAR:
					statement.setString(i, StringHelper.trunc((String)result, 1));
					break;
				case Types.DATE:
					statement.setDate(i, (java.sql.Date)result);
					break;
				case Types.DECIMAL:
					statement.setDouble(i, ((Double)result).doubleValue());
					break;
				case Types.DOUBLE:
					statement.setDouble(i, ((Double)result).doubleValue());
					break;
				case Types.FLOAT:
					statement.setFloat(i, ((Float)result).floatValue());
					break;
				case Types.INTEGER:
					statement.setInt(i, ((Integer)result).intValue());
					break;
				case Types.LONGVARCHAR:
					statement.setString(i, StringHelper.trunc((String)result, fields[j].getFieldLength()));
					break;
				case Types.REAL:
					statement.setDouble(i, ((Double)result).doubleValue());
					break;
				case Types.SMALLINT:
					statement.setInt(i, ((Boolean)result).booleanValue()?1: 0);
					break;
				case Types.TIMESTAMP:
					statement.setTimestamp(i, DateHelper.getTimestamp((Date)result));
					break;
				case Types.TINYINT:
					statement.setInt(i, ((Integer)result).intValue());
					break;
				case Types.VARCHAR:
					statement.setString(i, StringHelper.trunc((String)result, fields[j].getFieldLength()));
					break;
				default:
					Log.log(1, "SPSV:  Couldnt't find datatype for " + fields[j].getFieldName() + " type : " + fields[j].getFieldType());
			}
			
		}
	}
	
	private static String extractSimpleName(Class c)
	{
		String className = c.getName();
		int index = className.lastIndexOf(".");
		return className.substring(index + 1);
	}
	
	private static String getTableNameFromClass(Class c) throws Exception
	{
		
		WscBean wscBean = (WscBean)c.newInstance();
//		if (toLowerCase)
			return wscBean.getTableName().toLowerCase();
//		else
//			return wscBean.getTableName();
	}
	
	
}
