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
package it.bancaditalia.oss.vtl.impl.engine.exceptions;

import java.util.stream.Collectors;

import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Tokenmapping;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Tokenset;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Valueparam;

public class VTLUnmappedTokenException extends RuntimeException
{
	private static final long serialVersionUID = 1L;
	private final String sourceToken;
	private final Tokenset tokenset;

	public VTLUnmappedTokenException(String sourceToken, Tokenset tokenset)
	{
		super("Token " + sourceToken + " is not mapped in tokenset " + tokenset.getTokenmapping().stream().map(Tokenmapping::getName).collect(Collectors.joining(", ")));
		
		this.sourceToken = sourceToken;
		this.tokenset = tokenset;
	}
	
	public VTLUnmappedTokenException(String sourceToken, Valueparam param)
	{
		super("In valueparam " + param.getName() + ", token '" + sourceToken + "' cannot be mapped to a ScalarValue type.");

		this.sourceToken = sourceToken;
		this.tokenset = null;
	}
	
	public String getSourceToken()
	{
		return sourceToken;
	}
	
	public Tokenset getTokenset()
	{
		return tokenset;
	}
}
