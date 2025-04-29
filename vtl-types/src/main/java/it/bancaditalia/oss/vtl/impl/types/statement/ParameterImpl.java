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
package it.bancaditalia.oss.vtl.impl.types.statement;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.transform.Parameter;
import it.bancaditalia.oss.vtl.model.transform.ParameterType;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

/**
 * Represent a parameter definition for a VTL custom operator
 * 
 * @author Valentino Pinna
 */
public class ParameterImpl implements Parameter, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final VTLAlias name;
	private final ParameterType type;

	public ParameterImpl(VTLAlias name, ParameterType type)
	{
		this.name = name;
		this.type = type;
	}
	
	@Override
	public final VTLAlias getAlias()
	{
		return name;
	}

	@Override
	public boolean matches(TransformationScheme scheme, Transformation argument)
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ParameterType getParameterType()
	{
		return type;
	}
	
	@Override
	public String toString()
	{
		return name + " " + type;
	}
}
