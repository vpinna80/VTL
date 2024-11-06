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
package it.bancaditalia.oss.vtl.impl.engine.statement;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.transform.Parameter;

/**
 * Represent a parameter definition for a VTL custom operator
 * 
 * @author Valentino Pinna
 */
public abstract class BaseParameter implements Parameter, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final VTLAlias name;

	public BaseParameter(VTLAlias name)
	{
		this.name = name;
	}
	
	public VTLAlias getAlias()
	{
		return name;
	}
}
