/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.transform.exceptions;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;

public class VTLIncompatibleMeasuresException extends VTLException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final DataStructureComponent<? extends Measure, ?, ?> left;
	private final DataStructureComponent<? extends Measure, ?, ?> right;
	
	public VTLIncompatibleMeasuresException(String operation, DataStructureComponent<? extends Measure, ?, ?> left, DataStructureComponent<? extends Measure, ?, ?> right)
	{
		super("Incompatible measures in " + operation + ": " + left + ", " + right, null);
		
		this.left = left;
		this.right = right;
	}
	
	public DataStructureComponent<? extends Measure, ?, ?> getLeftDomain()
	{
		return left;
	}
	
	public DataStructureComponent<? extends Measure, ?, ?> getRightDomain()
	{
		return right;
	}
}
