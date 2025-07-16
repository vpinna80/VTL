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

import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_FOLDER;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_MULTIPLE;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_PASSWORD;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_REQUIRED;

import java.util.Set;

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
	public enum Options {
		IS_PASSWORD, IS_REQUIRED, IS_MULTIPLE, IS_FOLDER, IS_URL
	}
	
	/**
	 * @return The name of the system property that provides a starting value
	 */
	public String getName();

	/**
	 * @return A description of the property
	 */
	public String getDescription();

	/**
	 * @return A placeholder for the property that can be used as a hint for the property contents
	 */
	public String getPlaceholder();

	/**
	 * @return The default value for this property
	 */
	public String getDefaultValue();

	/**
	 * @return the set of configured options for this VTLProperty.
	 */
	public Set<Options> getOptions();

	/**
	 * @return {@code true} if the property allows multiple values
	 */
	public default boolean isMultiple()
	{
		return getOptions().contains(IS_MULTIPLE);
	}
	
	/**
	 * @return {@code true} if the property represents a password
	 */
	public default boolean isPassword()
	{
		return getOptions().contains(IS_PASSWORD);
	}
	
	/**
	 * @return {@code true} if the property must have a value set before using the component
	 */
	public default boolean isRequired()
	{
		return getOptions().contains(IS_REQUIRED);
	}
	
	/**
	 * @return {@code true} if the property represents a folder name
	 */
	public default boolean isFolder()
	{
		return getOptions().contains(IS_FOLDER);
	}
}