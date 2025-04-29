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
package it.bancaditalia.oss.vtl.model.transform;

public interface ParameterType
{
	/**
	 * 
	 * @return true if this parameter has a dataset type
	 */
	public boolean isDataSetParameter();
	
	/**
	 * Checks if the provided metadata argument matches this ParameterType definition
	 * @param scheme The transformation scheme in which the match should be tested
	 * @param argument The argument expression to match to this parameter
	 * 
	 * @return true if the check is positive
	 */
	public boolean matches(TransformationScheme scheme, Transformation argument);
}
