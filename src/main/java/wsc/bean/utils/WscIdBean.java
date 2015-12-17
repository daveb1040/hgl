package wsc.bean.utils;


import java.sql.Connection;

public abstract class WscIdBean extends WscBean {

	private long id;

	protected WscIdBean(WscDbContext context, long id)
	{	
		super(context);
		this.id = id;		
	}
	
	public WscIdBean(WscDbContext context)
	{
		super(context);
		id = getNextUniqueId();
	}
	public String getIdFieldName()
	{
		return "id";
	}
	
	public long getId() {
		return id;
	}
	
}
