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

import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_MULTIPLE;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.joining;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;

/**
 * This enum exposes the main configuration properties for the VTL Engine.
 * 
 * Each property enables you to choose an implementation for each of the modules 
 * of which the VTL Engine is composed.
 * 
 * @author Valentino Pinna
 */
public enum VTLGeneralProperties implements VTLProperty  
{
	/**
	 * This property allows to choose how to manage the VTL configuration.
	 * 
	 * The default value may be changed by using the {@code vtl.config.impl.class} system property. 
	 */
	CONFIG_MANAGER("vtl.config.impl.class", false, "it.bancaditalia.oss.vtl.impl.config.ConfigurationManagerImpl"),

	/**
	 * This property allows to choose which {@link MetadataRepository metadata repository} should be used by the Engine.
	 * 
	 * The default value may be changed by setting the {@code vtl.metadatarepository.class} system property. 
	 */
	METADATA_REPOSITORY("vtl.metadatarepository.class", false, "it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository"),

	/**
	 * This property allows to choose which {@link Engine computing engine} should be used.
	 * 
	 * The default value may be changed by setting the {@code vtl.engine.implementation.class} system property. 
	 */
	ENGINE_IMPLEMENTATION("vtl.engine.implementation.class", false, "it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine"),

	/**
	 * This property allows to choose which {@link VTLSession VTL session} should be used by the Engine.
	 * 
	 * The default value may be changed by using the {@code vtl.session.implementation.class} system property. 
	 */
	SESSION_IMPLEMENTATION("vtl.session.implementation.class", false, "it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl"),

	/**
	 * This property allows to choose which {@link Environment environments} should be used by the Engine.
	 * 
	 * The default value may be changed by using the {@code vtl.environment.implementation.classes} system property to
	 * a sequence of class names separated by comma. 
	 */
	ENVIRONMENT_IMPLEMENTATION("vtl.environment.implementation.classes", true, 
			"it.bancaditalia.oss.vtl.impl.environment.CSVFileEnvironment",
			"it.bancaditalia.oss.vtl.impl.environment.WorkspaceImpl"),

	/**
	 * This property allows to choose whether to use BigDecimal as the internal java representation of values of domain Number.
	 * 
	 * The default value may be changed by using the {@code vtl.config.use.bigdecimal} system property. 
	 */
	USE_BIG_DECIMAL("vtl.config.use.bigdecimal", false, "false");

	public static boolean isUseBigDecimal()
	{
		return Boolean.valueOf(USE_BIG_DECIMAL.getValue());
	}
	
	private final String name;
	private final Set<Options> options;
	private final String defaultValue;
	
	private String value = null;

	private VTLGeneralProperties(String name, boolean multiple, String... defaultValue)
	{
		this.name = name;
		this.options = multiple ? EnumSet.of(IS_MULTIPLE) : emptySet();
		
		this.defaultValue = Arrays.stream(defaultValue).collect(joining(","));
	}

	@Override
	public String getName()
	{
		return name;
	}
	
	@Override
	public String getValue()
	{
		return value == null || value.isEmpty() ? System.getProperty(name, defaultValue) : value;
	}

	@Override
	public void setValue(Object newValue)
	{
		value = Objects.toString(newValue);
	}
	
	public Set<Options> getOptions()
	{
		return options;
	}

	@Override
	public String getDescription()
	{
		return "";
	}

	@Override
	public String getPlaceholder()
	{
		return "";
	}
	
	@Override
	public boolean hasValue()
	{
		return true;
	}
}