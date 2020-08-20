package it.bancaditalia.oss.vtl.config;

import static java.util.Collections.singletonList;

import java.util.Arrays;
import java.util.List;

/**
 * This interface provides access to the configuration properties of the VTL Engine components.
 * 
 * Each property has a default initial value, that can be overriden by setting a value for the 
 * linked system property.
 * 
 * @author Valentino Pinna
 */
public interface VTLProperty
{
	/**
	 * @return The name of the system property that provides a starting value
	 */
	public String getName();

	/**
	 * @return The current value for this property
	 */
	public String getValue();

	/**
	 * Change the value for this property
	 * 
	 * @param newValue The new value for this property
	 */
	public void setValue(String newValue);

	/**
	 * @return A description of the property
	 */
	public String getDescription();

	/**
	 * @return A placeholder for the property that can be used as a hint for the property contents
	 */
	public String getPlaceholder();

	/**
	 * @return {@code true} if the property allows multiple values
	 */
	public boolean isMultiple();

	/**
	 * @return {@code true} if the property must have a value set before using the component
	 */
	public boolean isRequired();

	/**
	 * @return If the property {@link #isMultiple()}, a {@link List} where the elements 
	 * 		match each of this property's values
	 */
	public default List<String> getValues()
	{
		return getValue() == null ? null : isMultiple() ? Arrays.asList(getValue().split(",")) : singletonList(getValue());
	}
}