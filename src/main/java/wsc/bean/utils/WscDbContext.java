package wsc.bean.utils;

import java.sql.Connection;

public interface WscDbContext 
{
	public Connection getConnection();
	public String getSchemaName();
}
