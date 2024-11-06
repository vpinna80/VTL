package it.bancaditalia.oss.vtl.model.data;

import java.util.Map.Entry;

/**
 * A interface that captures the behavior of VTL identifiers
 */
public interface VTLAlias
{
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
	 * @return  the composed print name.
	 */
	@Override
	String toString();
	
	@Override
	int hashCode();
	
	@Override
	boolean equals(Object obj);

}