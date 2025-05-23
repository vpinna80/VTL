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
package it.bancaditalia.oss.vtl.engine;

import java.util.List;

import it.bancaditalia.oss.vtl.model.transform.Parameter;
import it.bancaditalia.oss.vtl.model.transform.ParameterType;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

/**
 * TODO: Specification unclear
 * 
 * @author Valentino Pinna
 *
 */
public interface UserOperatorStatement extends DDLStatement
{
	/**
	 * 
	 * @return The list of formal parameters to this operator
	 */
	public List<Parameter> getParameters();

	/**
	 * 
	 * @return The type returned by this operator
	 */
	public ParameterType getReturnType();
	
	/**
	 * 
	 * @return The expression used to compute the result of this operator
	 */
	public Transformation getExpression();
}
