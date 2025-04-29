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
package it.bancaditalia.oss.vtl.impl.transform.statement;

import static it.bancaditalia.oss.vtl.impl.types.statement.AssignmentType.PERSISTENT;
import static java.util.function.Predicate.not;

import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.engine.DMLStatement;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.types.statement.AbstractStatement;
import it.bancaditalia.oss.vtl.impl.types.statement.AssignmentType;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class AssignStatement extends AbstractStatement implements DMLStatement
{
	//private final static Logger LOGGER = LoggerFactory.getLogger(AssignStatement.class);
	private static final long serialVersionUID = 1L;

	private final Transformation expression;
	private final boolean persistent;

	public AssignStatement(AssignmentType type, VTLAlias alias, Transformation expression)
	{
		super(alias);
		this.expression = expression;
		this.persistent = type == PERSISTENT;
	}

	public Transformation getExpression()
	{
		return expression;
	}

	@Override
	public String toString()
	{
		return getAlias() + (isPersistent() ? " <- " : " := ") + expression;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return expression.getTerminals();
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		try
		{
			final VTLValue value = expression.eval(scheme);
			if (isPersistent())
				scheme.persist(value, getAlias());
			return value;
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
	public VTLValueMetadata getMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata structure = expression.getMetadata(scheme);
		
		if (persistent)
			try
			{
				Optional<VTLValueMetadata> repoMeta = scheme.getRepository().getMetadata(getAlias());
				if (repoMeta.filter(not(structure::equals)).isPresent())
					throw new VTLException("Dataset " + getAlias() + repoMeta.get() + " cannot be assigned an expression of type " + structure);
			}
			catch (VTLException e)
			{
				throw new VTLNestedException("Error evaluating statement '" + this + "'. Error: " + e.getMessage(), e);
			}
		
		return structure;
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}
	
	@Override
	public boolean isCacheable()
	{
		return !persistent;
	}
}