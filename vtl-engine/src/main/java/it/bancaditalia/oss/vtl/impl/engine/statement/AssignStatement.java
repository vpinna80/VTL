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

import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

class AssignStatement extends AbstractStatement implements Transformation
{
	//private final static Logger LOGGER = LoggerFactory.getLogger(AssignStatement.class);
	private static final long serialVersionUID = 1L;

	private final Transformation	expression;
	private final boolean			persistent;

	public AssignStatement(String name, Transformation expression, boolean persistent)
	{
		super(name);
		this.expression = expression;
		this.persistent = persistent;
	}

	public Transformation getExpression()
	{
		return expression;
	}

	@Override
	public String toString()
	{
		return getId() + (isPersistent() ? " <- " : " := ") + expression;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return expression.getTerminals();
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		try
		{
			return expression.eval(session);
		}
		catch (VTLException e)
		{
			throw new VTLNestedException("Error evaluating statement '" + this + "'", e);
		}
	}
	
	public boolean isPersistent()
	{
		return persistent;
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		try
		{
			return expression.getMetadata(session);
		}
		catch (VTLException e)
		{
			throw new VTLNestedException("Error evaluating statement '" + this + "'. Error: " + e.getMessage(), e);
		}
	}

	@Override
	public Lineage getLineage()
	{
		return LineageNode.of(this, expression.getLineage());
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}
}