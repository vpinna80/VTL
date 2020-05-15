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
package it.bancaditalia.oss.vtl.impl.engine.exceptions;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;

import it.bancaditalia.oss.vtl.exceptions.VTLException;

public class VTLUnmappedContextException extends VTLException
{
	private static final long serialVersionUID = 1L;
	private final ParserRuleContext context;
		
	public VTLUnmappedContextException(ParserRuleContext ctx)
	{
		super("No mapping found for " + ctx.getClass().getSimpleName() + " on " + 
				ctx.start.getInputStream().getText(new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex())));
		
		this.context = ctx;
	}
	
	public ParserRuleContext getContext()
	{
		return context;
	}
}
