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
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;

public class VTLInvariantIdentifiersException extends VTLException
{
	private static final long serialVersionUID = 1L;

	public VTLInvariantIdentifiersException(String operation, Set<? extends DataStructureComponent<? extends Identifier, ?, ?>> before)
	{
		super(operation + " cannot change identifiers " + before);
	}

	public VTLInvariantIdentifiersException(String operation, DataStructureComponent<? extends Identifier, ?, ?> before, Class<? extends Component> role)
	{
		super(operation + " cannot change identifier " + before + " to a " + role.getSimpleName());
	}

	public VTLInvariantIdentifiersException(String operation, DataStructureComponent<?, ?, ?> before)
	{
		super(operation + " cannot change " + before + " to an identifier.");
	}

	public VTLInvariantIdentifiersException(String operation, Set<? extends DataStructureComponent<? extends Identifier, ?, ?>> before, Set<? extends DataStructureComponent<? extends Identifier, ?, ?>> after)
	{
		super(operation + " cannot change identifiers from " + before + " to " + after);
	}

	public VTLInvariantIdentifiersException(String operation, DataStructureComponent<? extends Identifier, ?, ?> before, String after)
	{
		super(operation + " cannot change identifiers from " + before + " to " + after);
	}
}
