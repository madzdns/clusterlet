package com.github.madzdns.clusterlet.helper;

import java.util.Collection;
import java.util.Iterator;

public class CollectionHelper {

	public static <E> boolean containsAny(Collection<E> c1,Collection<E> c2) {
		
		if(c1==null&&c2==null) {
			
			return true;
		}
			
		if(c1==null||c2==null) {
			
			return false;
		}
			
		for(Iterator<E> it1 = c1.iterator();it1.hasNext();) {
			
			E item1 = it1.next();
			for(Iterator<E> it2 = c2.iterator();it2.hasNext();) {
				
				if(item1.equals(it2.next()))
					return true;
			}
		}
		return false;
	}
}
