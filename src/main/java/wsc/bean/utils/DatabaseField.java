package wsc.bean.utils;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import weidenhammer.com.utils.DateHelper;

public class DatabaseField {
    private String fieldName;
    private String tableName;
    private int fieldType;
    private int fieldLength;



    public final static int COLUMN_NAME = 0;
    public final static int BOTH = 1;



    public DatabaseField(String field, String table, int type, int length)
    {
        fieldName = field;
        tableName = table;
        fieldType = type;
        fieldLength = length;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getFieldType() {
        return fieldType;
    }

    public void setFieldType(int fieldType) {
        this.fieldType = fieldType;
    }

    public int getFieldLength() {
        return fieldLength;
    }

    public void setFieldLength(int fieldLength) {
        this.fieldLength = fieldLength;
    }

    /**
     * Returns the table name concatanated with the field name.
     * @return
     * <br><br>  Students, student_id, would return Students.student_id
     */
    public String getDatabaseFieldName()
    {
        return getTableName() + "." + getFieldName();
    }

    /**
     * Extracts the value of the database field from the current reuulst set
     * @param rs
     * @return
     */
    public String extractRSValue(ResultSet rs) throws SQLException
    {
        return rs.getString(getDatabaseFieldName());
    }


    public String extractRSValue(ResultSet rs, int nameType) throws SQLException
    {
        if (nameType == BOTH)
            return rs.getString(getDatabaseFieldName());
        else
            return rs.getString(getFieldName());

    }
    
    public String toString()
    {
    	return "  fieldName := " + fieldName + " tableName := " + tableName + " fieldType := " + fieldType + " fieldLength := " + fieldLength; 
    }
    
    public void addToPreparedStatement(PreparedStatement statement, int index, Object value) throws Exception
    {
    	switch(this.fieldType)
    	{
    	case Types.ARRAY:
    		//statement.setArray(value);
    		break;
    	case Types.BIGINT:
    		statement.setLong(index, Long.parseLong((String)value));
    		break;
    	case Types.BINARY:
    		statement.setInt(index, Integer.parseInt((String)value));
    		break;
    	case Types.BIT:
    		statement.setInt(index, Integer.parseInt((String)value));
    		break;
    	case Types.BLOB:
//    		statement.setBlob(value);
    		break;
    	case Types.BOOLEAN:
    		statement.setBoolean(index, Boolean.parseBoolean((String)value));
    		break;
    	case Types.CHAR:
    		statement.setString(index, (String)value);
    		break;
    	case Types.CLOB:
//    		statement.setClob(index, value);
    		break;
    	case Types.DATALINK:
//    		statement.set(value);
    		break;
    	case Types.DATE:
    		statement.setDate(index, (Date)value);
    		break;
    	case Types.DECIMAL:
    		statement.setFloat(index, Float.parseFloat((String)value));
    		break;
    	case Types.DISTINCT:
//    		statement.set(value);
    		break;
    	case Types.DOUBLE:
    		statement.setDouble(index, Double.parseDouble((String)value));
    		break;
    	case Types.FLOAT:
    		statement.setFloat(index, Float.parseFloat((String)value));
    		break;
    	case Types.INTEGER:
    		statement.setInt(index, Integer.parseInt((String)value));
    		break;
    	case Types.JAVA_OBJECT:
//    		statement.set(value);
    		break;
    	case Types.LONGVARBINARY:
//    		statement.set(value);
    		break;
    	case Types.LONGVARCHAR:
    		statement.setString(index, (String)value);
    		break;
    	case Types.NULL:
//    		statement.set(value);
    		break;
    	case Types.NUMERIC:
//    		statement.set(value);
    		break;
    	case Types.OTHER:
//    		statement.set(value);
    		break;
    	case Types.REAL:
//    		statement.set(value);
    		break;
    	case Types.REF:
//    		statement.set(value);
    		break;
    	case Types.SMALLINT:
    		statement.setInt(index, Integer.parseInt((String)value));
    		break;
    	case Types.STRUCT:
//    		statement.set(value);
    		break;
    	case Types.TIME:
//    		statement.set(value);
    		break;
    	case Types.TIMESTAMP:
    		statement.setTimestamp(index, DateHelper.getTimestamp((Date)value));
    		break;
    	case Types.TINYINT:
    		statement.setInt(index, Integer.parseInt((String)value));
    		break;
    	case Types.VARBINARY:
//    		statement.set(value);
    		break;
    	case Types.VARCHAR:
    		statement.setString(index, (String)value);
    		break;
    	}
    }
}
