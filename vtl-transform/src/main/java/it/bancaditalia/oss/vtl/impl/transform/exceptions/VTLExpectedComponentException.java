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
package it.bancaditalia.oss.vtl.impl.transform.exceptions;

import java.util.Collection;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;

public class VTLExpectedComponentException extends VTLException
{
	private static final long serialVersionUID = 1L;

	public VTLExpectedComponentException(Class<? extends ComponentRole> role, ValueDomainSubset<?, ?> domain, Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		super("Expected exactly one " + role.getSimpleName() + " of type " + domain + " but found: " + components);
	}

	public VTLExpectedComponentException(Class<? extends ComponentRole> role, Domains<?, ?> domain, Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		super("Expected exactly one " + role.getSimpleName() + " of type " + domain + " but found: " + components);
	}

	public VTLExpectedComponentException(Class<? extends ComponentRole> role, Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		super("Expected exactly one " + role.getSimpleName() + " but found " + components);
	}
}
