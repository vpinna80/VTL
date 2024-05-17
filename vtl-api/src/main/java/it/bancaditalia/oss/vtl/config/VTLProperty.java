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
package it.bancaditalia.oss.vtl.config;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;

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
	 * @return {@code true} if the property represents a password
	 */
	public boolean isPassword();
	
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

	/**
	 * Change the value for this property
	 * 
	 * @param newValue The new value for this property
	 */
	public default void setValue(Class<?> newValue)
	{
		setValue(newValue.getName());
	}
	
	/**
	 * Change the values for this property. If the property is not multiple, behaviour is undefined
	 * 
	 * @param newValues The new values for this property
	 */
	public default void setValues(String... newValues)
	{
		setValue(Arrays.stream(newValues).collect(joining(",")));
	}

	/**
	 * Change the values for this property. If the property is not multiple, behaviour is undefined
	 * 
	 * @param newValues The new values for this property
	 */
	public default void setValues(Class<?>... newValues)
	{
		setValue(Arrays.stream(newValues).map(Class::getName).collect(joining(",")));
	}

	/**
	 * @return {@code true} if a value was set for this property or it has a default value 
	 */
	public boolean hasValue();

	/**
	 * Add new values to this property. If the property is not multiple, behaviour is undefined
	 * 
	 * @param newValues The new values to add to existing values of this property
	 */
	public default void addValues(String... newValues)
	{
		String beginning = hasValue() ? getValue() + "," : "";
		setValue(Arrays.stream(newValues).collect(joining(",", beginning, "")));
	}
	
	/**
	 * Verifies if this and/or other properties match a custom rule.
	 * By default this method returns true.
	 * Implementers should override this method to check if the value satisfies a speficic rule.
	 * 
	 * @return true if this property value satisfies a speficic rule.
	 */
	public default boolean validate() 
	{
		return true;
	}
}