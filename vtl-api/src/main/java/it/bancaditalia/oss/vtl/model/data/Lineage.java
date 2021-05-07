package it.bancaditalia.oss.vtl.model.data;

import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

/**
 * Represents a lineage of a DataPoint.
 * 
 * @author Valentino Pinna
 */
public interface Lineage
{
	/**
	 * Resolve lineage links to aliases bound to a {@link TransformationScheme}.
	 * 
	 * @param scheme the scheme containing the required external bindings 
	 * @return A new Lineage with all external links resolved.
	 */
	public Lineage resolveExternal(TransformationScheme scheme);
}
