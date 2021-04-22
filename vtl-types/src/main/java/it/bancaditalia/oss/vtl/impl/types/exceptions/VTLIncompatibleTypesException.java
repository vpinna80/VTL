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
package it.bancaditalia.oss.vtl.impl.types.exceptions;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;

public class VTLIncompatibleTypesException extends VTLException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final ValueDomain left, right;
	
	public VTLIncompatibleTypesException(String operation, ValueDomain left, ValueDomain right)
	{
		super("Found incompatible types in " + operation + ": " + left + ", " + right, null);
		
		this.left = left;
		this.right = right;
	}
	
	public VTLIncompatibleTypesException(String operation, Domains left, ValueDomain right)
	{
		super("Found incompatible types in " + operation + ": " + left.getDomain() + ", " + right, null);
		
		this.left = left.getDomain();
		this.right = right;
	}
	
	public VTLIncompatibleTypesException(String operation, ValueDomain left, Domains right)
	{
		super("Found incompatible types in " + operation + ": " + left + ", " + right.getDomain(), null);
		
		this.left = left;
		this.right = right.getDomain();
	}
	
	public VTLIncompatibleTypesException(String operation, DataStructureComponent<?, ?, ?> left, ValueDomain right)
	{
		super("Found incompatible types in " + operation + ": " + left + ", " + right, null);
		
		this.left = left.getDomain();
		this.right = right;
	}
	
	public VTLIncompatibleTypesException(String operation, ValueDomain left, DataStructureComponent<?, ?, ?> right)
	{
		super("Found incompatible types in " + operation + ": " + left + ", " + right, null);
		
		this.left = left;
		this.right = right.getDomain();
	}
	
	public VTLIncompatibleTypesException(String operation, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		this(operation, left.getDomain(), right.getDomain());
	}
	
	public ValueDomain getLeftDomain()
	{
		return left;
	}
	
	public ValueDomain getRightDomain()
	{
		return right;
	}
}
