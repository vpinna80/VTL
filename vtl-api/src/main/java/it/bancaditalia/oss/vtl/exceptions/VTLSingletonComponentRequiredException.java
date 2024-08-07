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
package it.bancaditalia.oss.vtl.exceptions;

import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public class VTLSingletonComponentRequiredException extends VTLException
{
	private static final long serialVersionUID = 1L;

	public VTLSingletonComponentRequiredException(Class<? extends Component> role, Set<? extends DataStructureComponent<?, ?, ?>> components)
	{
		super("Required exactly one " + role.getSimpleName() + " but found: " + components);
	}

	public VTLSingletonComponentRequiredException(Class<? extends Component> role, ValueDomain domain, Set<? extends DataStructureComponent<?, ?, ?>> components)
	{
		super("Required exactly one " + role.getSimpleName() + " of domain " + domain + " but found: " + components);
	}
}
