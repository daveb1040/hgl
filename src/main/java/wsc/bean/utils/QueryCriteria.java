package wsc.bean.utils;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Enumeration;
import java.util.Hashtable;


import weidenhammer.com.utils.ArrayHelper;
import weidenhammer.com.utils.Log;
import weidenhammer.com.utils.Queue;
import weidenhammer.com.utils.ReflectionHelper;

public class QueryCriteria {

    private String searchValues[][];
    private int expandBy = 10;
    private DatabaseField fieldNames[];
    private int comparators[][];
    private Queue orderByValues = new Queue();
    private WscDbContext wscDbContext;

    public final static int EQUALS = 0;
    public final static int LESS_THAN = 1;
    public final static int LESS_THAN_EQUAL_TO = 2;
    public final static int GREATER_THAN = 3;
    public final static int GREATER_THAN_EQUAL_TO = 4;
    public final static int DOES_NOT_EQUAL = 5;
    public final static int LIKE = 6;

    public final static boolean NOT = true;
    public final static int EXPAND_BY = 5;

    public final static int DEFAULT_COMPARISON = EQUALS;

    public final static long TRUE = 1;
    public final static long FALSE = 0;

    public QueryCriteria(WscDbContext wscDbContext, DatabaseField localFieldNames[])
    {
        searchValues = new String[localFieldNames.length][EXPAND_BY];
        comparators = new int[localFieldNames.length][EXPAND_BY];
        fieldNames = localFieldNames;
        setWscDbContext(wscDbContext);
    }

    public static String getOperator(int operator)
    {
        String returnValue;
        switch(operator)
        {
            case EQUALS:
                returnValue = "=";
                break;
            case LESS_THAN:
                returnValue = "<";
                break;
            case LESS_THAN_EQUAL_TO:
                returnValue = "<=";
                break;
            case GREATER_THAN:
                returnValue = ">";
                break;
            case GREATER_THAN_EQUAL_TO:
                returnValue = ">=";
                break;
            case DOES_NOT_EQUAL:
                returnValue = "!=";
                break;
            case LIKE:
                returnValue = " LIKE ";
                break;
            default:
                returnValue = "=";
         }
        return returnValue;
    }

    public static String wrapInNot(String statement)
    {
        return " NOT " + wrapInBrackets(statement);
    }

    public static String wrapInBrackets(String statement)
    {
        return " ( " + statement + " ) ";
    }

    public static String getWhereClauseFormat(String fieldValue, DatabaseField databaseField)
    {
        return getWhereClauseFormat(fieldValue, databaseField, DEFAULT_COMPARISON);
    }

    public static String getWhereClauseFormat(String fieldValue, DatabaseField databaseField, int operator)
    {
        return getWhereClauseFormat(fieldValue, databaseField, operator, !NOT);
    }

    public static String getWhereClauseFormat(String fieldValue, DatabaseField databaseField, int operator, boolean notTrue)
    {
        String returnString =  " " + databaseField.getFieldName() + " " + getOperator(operator) + " ? ";
        if (notTrue)
            returnString = wrapInNot(returnString);
        return returnString;
    }

    public String getWhereClauseForList(int index)
    {
        String returnValue = "";
        boolean addedOne = false;
        if (searchValues[index] != null)
            for(int i = 0; i < searchValues[index].length && searchValues[index][i] != null; i++)
            {
                if (addedOne)
                    returnValue += " or ";
                else
                    addedOne = true;
                returnValue += getWhereClauseFormat(searchValues[index][i], fieldNames[index], comparators[index][i]);
            }
        if (addedOne)
            returnValue = wrapInBrackets(returnValue);
        return returnValue;
    }


    public String getWhereClause()
    {
        String whereClause = "";
        boolean addedOne = false;
        for(int i = 0; i < fieldNames.length; i++)
            if (fieldNames[i] != null)
            {
                String whereClauseFromList = getWhereClauseForList(i);
                if (whereClauseFromList.trim().length() != 0)
                    if (addedOne)
                    {
                        whereClause += " and ";
                    }
                    else
                        addedOne = true;
                whereClause += whereClauseFromList;
            }
        if (whereClause.trim().length() != 0)
        	whereClause = "Where " + whereClause;
        return whereClause;
    }

    /**********************************************************************
     *          A d d   S e r a c h   V a l u e s 
     **********************************************************************/
    
    public void addSearchValues(int searchID, long values[])
    {
        if(values != null)
            for (int i = 0; i < values.length; i++)
                addSearchValues(searchID, values[i]);
    }

    public void addSearchValues(int searchID, long searchValue)
    {
        addSearchValues(searchID, searchValue, DEFAULT_COMPARISON);
    }

    public void addSearchValues(int searchId, long searchValue, int comparator)
    {
    	addSearchValues(searchId, searchValue + "", comparator);
    }
    
    public void addSearchValues(int searchID, String values[])
    {
        if(values != null)
            for (int i = 0; i < values.length; i++)
                addSearchValues(searchID, values[i], DEFAULT_COMPARISON);
    }

    public void addSearchValues(int searchID, String searchValue)
    {
        addSearchValues(searchID, searchValue, DEFAULT_COMPARISON);
    }

    public void addSearchValues(int searchID, String searchValue, int comparator)
    {
        int i = 0;
        
        for(; i < searchValues[searchID].length && searchValues[searchID][i] != null; i++)
    	{
    		int doNothing;
    	}

        if (i == searchValues[searchID].length)
        {
            searchValues[searchID] = expandArray(searchValues[searchID], EXPAND_BY);
            comparators[searchID] = expandArray(comparators[searchID], EXPAND_BY);
        }

        searchValues[searchID][i] = searchValue;
        comparators[searchID][i] = comparator;
    }

    
    /**********************************************************************
     *          A d d   O r d e r   V a l u e s 
     **********************************************************************/
    
    public void addOrderByValue(int field)
    {
    	orderByValues.offer(fieldNames[field]);
    }
    
    public void addSearchValues(int searchID, String[] values, int comparator)
    {
        if(values != null)
            for (int i = 0; i < values.length; i++)
                addSearchValues(searchID, values[i], comparator);
    }

    public static String[] expandArray(String array[], int expandBy)
    {
        String newArray[] = new String[array.length + expandBy];
        for(int i = 0; i < array.length; i++)
            newArray[i] = array[i];
        return newArray;
    }

    private static int[] expandArray(int[] array, int expandBy)
    {
        int newArray[] = new int[array.length + expandBy];
        for(int i = 0; i < array.length; i++)
            newArray[i] = array[i];
        return newArray;
    }

    public DatabaseField[] getFieldNames()
    {
        return fieldNames;
    }

    public ResultSet executeQuery() throws Exception
    {
    	String selectClause = getSelectClause();
    	String fromClause = getFromClause();
    	String whereClause = getWhereClause();
    	String orderByClause = getOrderByClause();
    	
    	String sqlString = selectClause + " " + fromClause + " " + whereClause + " " + orderByClause; 
	    Log.log(5, "The sql statement := " + sqlString);	    
    	
	    PreparedStatement statement = getWscDbContext().getConnection().prepareStatement(sqlString);
		int index = 0;
		for(int i = 0; i < searchValues.length; i++)
		{
	        if (searchValues[i] != null)
	            for(int j = 0; j < searchValues[i].length && searchValues[i][j] != null; j++)
	            	fieldNames[i].addToPreparedStatement(statement, ++index, searchValues[i][j]);
		}
		
//    	Log.log(10, "sqlString := " + sqlString);
    	
    	
    	return statement.executeQuery();
    }
    
    public String getSelectClause()
    {
    	Log.log(10, "FieldName := " + fieldNames);
    	String selectClause = "Select ";
    	for(int i = 0; i < fieldNames.length; i++)
    	{
    		if (i != 0)
    			selectClause += ", ";
    		selectClause += fieldNames[i].getTableName() + "." + fieldNames[i].getFieldName();
    	}
    	return selectClause;
    }
    
    public String getFromClause()
    {
    	String[] tableNames = getTableNames();
    	String fromClause = "From ";
    	for(int i = 0; i < tableNames.length; i++)
    	{
    		if (i > 0)
    			fromClause += ", ";
    		fromClause += tableNames[i];
    	}
    	return fromClause;
    }
    
    public String[] getTableNames()
    {
    	Hashtable tableNames = new Hashtable();
    	for(int i = 0; i < fieldNames.length; i++)
    		tableNames.put(fieldNames[i].getTableName(), fieldNames[i].getTableName());
    	Enumeration e = tableNames.elements();
    	int count = 0;
    	while(e.hasMoreElements())
    	{
    		e.nextElement();
    		count++;
    	}
    	String names[] = new String[count];
    	e = tableNames.elements();
    	for(int i = 0; e.hasMoreElements(); i++)
    		names[i] = e.nextElement().toString();
    	return names;
    }
    
    
    public String getOrderByClause()
    {
    	String orderByString = "";
    	Object[] obValues = orderByValues.toArray();
    	for(int i = 0; i < obValues.length; i++)
    	{
    		if (i == 0)
    			orderByString = "Order By ";
    		else
    			orderByString += ", ";
    		orderByString += ((DatabaseField)obValues[i]).getDatabaseFieldName();
    	}
    	return orderByString;
    }

    public Object[] findBy(Class beanClass) throws Exception
    {
    	Log.log(10, "FindBy := " + beanClass.getName());    	
    	Class[] types = new Class[2];
    	types[0] = WscDbContext.class;
    	types[1] = ResultSet.class;
    	Constructor constructor = ReflectionHelper.getContructor(beanClass,types);
    	
        ResultSet rs = executeQuery();
        Object[] params = new Object[2];
    	params[0] = getWscDbContext();
    	params[1] = rs;
        
        Object[] results = ArrayHelper.createArray(beanClass, 1000);
        int i = 0;
        for(i = 0; rs.next(); i++)
        {
             if (i == results.length)
                  results = (Object[])ArrayHelper.resizeArray(results, results.length + 200);
             results[i] = constructor.newInstance(params);
        }
        return (Object[])ArrayHelper.resizeArray(results, i);
    	
    }

	public void setWscDbContext(WscDbContext wscDbContext) {
		this.wscDbContext = wscDbContext;
	}

	public WscDbContext getWscDbContext() {
		return wscDbContext;
	}
}
