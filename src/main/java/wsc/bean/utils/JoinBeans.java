package wsc.bean.utils;
import java.lang.reflect.Method;

import weidenhammer.com.utils.ReflectionHelper;


public class JoinBeans 
{

	public static void leftOuterJoin(WscBean[] main, WscBean[] sub, String aSetMN, String aGetJoinIdMN, String bGetJoinIdMN) throws Exception
	{
		if (main == null || sub == null || main.length == 0 || sub.length == 0)
			return;
		
		leftOuterJoin(main, sub, ReflectionHelper.getMethod(main[0], aSetMN), 
				                 ReflectionHelper.getMethod(main[0], aGetJoinIdMN), 
				                 ReflectionHelper.getMethod(sub[0], bGetJoinIdMN));
	}

	public static void leftOuterJoin(WscBean[] main, WscBean[] sub, Method aSetMethod, Method aGetJoinIdMethod, Method bGetJoinIdMethod) throws Exception
	{
		if (main == null || sub == null || main.length == 0 || sub.length == 0)
			return;
		
		Class subComponentType = sub.getClass().getComponentType();		
		int subIndex = 0;
		for(int i = 0; i < main.length; i++)
		{
			long id = ((Long)aGetJoinIdMethod.invoke(main[i], null)).longValue();
			boolean foundFirstOne = false;
			boolean inList = true;
			//  Find first matching element in sub array
			while(subIndex < sub.length && !foundFirstOne && inList)
			{
				long subId = ((Long)bGetJoinIdMethod.invoke(sub[subIndex], null)).longValue();
				if (subId == id)
					foundFirstOne = true;
				else if (subId > id)
					inList = false;
				else
					subIndex++;
			}
			int newOneIndex = subIndex;

			if (foundFirstOne)
				for(boolean foundNewOne = false; newOneIndex < sub.length && !foundNewOne;)
					if (bGetJoinIdMethod.invoke(sub[newOneIndex], null).equals(Long.toString(id)))
						newOneIndex++;
					else
						foundNewOne = true;
			
			int arraySize = newOneIndex - subIndex;
			//  Create the new array, and set it to the element in the main array
			Object newArray = java.lang.reflect.Array.newInstance(subComponentType, arraySize);
			System.arraycopy (sub, subIndex, newArray, 0, arraySize);
			Object[] parameters = new Object[1];
			parameters[0] = newArray;
			aSetMethod.invoke(main[i], parameters);

			//  set the subIndex equal to the Next new index.
			subIndex = newOneIndex;
		}
	}
}
