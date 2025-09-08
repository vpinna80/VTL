/*
 * Copyright © 2020 Banca D'Italia
 *
 * Licensed under the EUPL, Version 1.2 (the "License");
 * You may not use this work except in compliance with the
 * License.
 * You may obtain a copy of the License at:
 *
 * https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the License is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 *
 * See the License for the specific language governing
 * permissions and limitations under the License.
 */
package it.bancaditalia.oss.vtl.model.data;

import java.util.Comparator;
import java.util.Map.Entry;

/**
 * A interface that captures the behavior of VTL identifiers
 */
public interface VTLAlias extends Comparable<VTLAlias>
{
	/**
	 * Compares two aliases by lowercased name. Quoting is not considered.
	 * 
	 * @return The comparator by name
	 */
	public static Comparator<VTLAlias> byName()
	{
		return (a, b) -> a == null ? -1 : b == null ? 1 : a.getMemberAlias().getName().toLowerCase().compareTo(b.getMemberAlias().getName().toLowerCase());
	}
	
	/**
	 * 
	 * @return the composed name of this alias. If a name element was quoted, quotes are removed  
	 */
	public String getName();
	
	/**
	 * 
	 * @return true if this alias has both a dataset name and a member name
	 */
	public boolean isComposed();

	/**
	 * If this alias is not composed, compose it to be a member of the dataset alias, if it's not composed.
	 * @param dataset a non-composed dataset name alias
	 * @return the composed alias
	 */
	public VTLAlias in(VTLAlias dataset);
	
	/**
	 * If this alias is composed, return only the member alias part. Otherwise return the input alias unchanged.
	 * @return the member alias part.
	 */
	public default VTLAlias getMemberAlias()
	{
		return this;
	}
	
	/**
	 * If this alias is composed, return the two elements composing it.
	 * 
	 * @return a pair of the two alias elements
	 * @throws UnsupportedOperationException if this alias is not composed.
	 */
	public default Entry<VTLAlias, VTLAlias> split()
	{
		throw new UnsupportedOperationException("This alias is not composed");
	}
	
	/**
	 * Creates the composed name of this alias for printing.
	 * The name is in the form 'dataset'#'member'. Single quotes are not removed from the original name.
	 * 
	 * @return  the composed print name.
	 */
	@Override
	String toString();
	
	@Override
	int hashCode();
	
	@Override
	boolean equals(Object obj);

}