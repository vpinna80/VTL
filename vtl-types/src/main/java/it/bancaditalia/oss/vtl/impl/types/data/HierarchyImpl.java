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
package it.bancaditalia.oss.vtl.impl.types.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.operators.ComparisonOperator;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Hierarchy;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.util.Utils;

// FIXME
public abstract class HierarchyImpl implements Hierarchy
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(HierarchyImpl.class);

//	private static final DoubleValue ZERO = new DoubleValue(0.0);
//	private static final DataStructureComponent<Measure, NumberDomainSubset<NumberDomain>, NumberDomain> imbalance = new DataStructureComponentImpl<>(
//			"imbalance", Measure.class, NUMBERDS);
//	private static final DataStructureComponent<Measure, BooleanDomainSubset, BooleanDomain> bool_var = new DataStructureComponentImpl<>("bool_var",
//			Measure.class, BOOLEANDS);
//	private static final DataStructureComponent<Measure, StringDomainSubset, StringDomain> errorcode = new DataStructureComponentImpl<>("errorcode",
//			Measure.class, STRINGDS);
//	private static final DataStructureComponent<Measure, IntegerDomainSubset, IntegerDomain> errorlevel = new DataStructureComponentImpl<>("errorlevel",
//			Measure.class, INTEGERDS);
//	private static final DataStructureComponent<Identifier, StringDomainSubset, StringDomain> ruleid = new DataStructureComponentImpl<>("ruleid",
//			Identifier.class, STRINGDS);

	public static class SourceItemImpl implements SourceItem
	{
		private final String id;
		private final boolean isWithRelation;
		private final Transformation condition;

		public SourceItemImpl(String id, boolean isWithRelation, Transformation condition)
		{
			this.id = id;
			this.isWithRelation = isWithRelation;
			this.condition = condition;
		}

		@Override
		public String getId()
		{
			return id;
		}

		@Override
		public boolean isWithRelation()
		{
			return isWithRelation;
		}

		@Override
		public Transformation getCondition()
		{
			return condition;
		}

		@Override
		public String toString()
		{
			return (isWithRelation ? "+" : "-") + id;
		}
	}

	public static class RuleItemImpl implements RuleItem
	{
		private final String codeItem;
		private final Transformation condition;
		private final ScalarValue<?,? extends IntegerDomainSubset,IntegerDomain> errorLevel;
		private final ScalarValue<?, ? extends StringDomainSubset, StringDomain> errorCode;
		private final List<? extends SourceItem> sourceItems;
		private final ComparisonOperator relation;

		public RuleItemImpl(String codeItem, ComparisonOperator relation, List<? extends SourceItem> sourceItems, Transformation condition,
				Transformation errorCode, Transformation errorLevel)
		{
			LOGGER.trace("Creating rule " + codeItem + " with errorCode " + errorCode + " errorlevel " + errorLevel);

			this.codeItem = codeItem;
			this.relation = relation;
			this.condition = condition;
			try
			{
				this.errorLevel = Domains.INTEGERDS.cast((ScalarValue<?, ?, ?>) errorLevel.eval(null));
				this.errorCode = Domains.STRINGDS.cast((ScalarValue<?, ?, ?>) errorCode.eval(null));
			}
			catch (VTLException e)
			{
				throw new VTLException("errorLevel and errorCode should be constants.", e);
			}
			this.sourceItems = sourceItems;
		}

		@Override
		public String getCodeItem()
		{
			return codeItem;
		}

		@Override
		public Transformation getCondition()
		{
			return condition;
		}

		@Override
		public ScalarValue<?,? extends IntegerDomainSubset,IntegerDomain> getErrorLevel()
		{
			return errorLevel;
		}

		@Override
		public ScalarValue<?, ? extends StringDomainSubset, StringDomain> getErrorCode()
		{
			return errorCode;
		}

		@Override
		public boolean contains(ScalarValue<? extends Object, ? extends StringDomainSubset, StringDomain> item)
		{
			return Utils.getStream(sourceItems).map(SourceItem::getId).anyMatch(i -> i.equals(item.get()));
		}

		@Override
		public int getSign(ScalarValue<?, ? extends StringDomainSubset, StringDomain> item)
		{
			return codeItem.equals(item.get()) ? 1
					: Utils.getStream(sourceItems).filter(s -> s.getId().equals(item.get())).findFirst().map(SourceItem::isWithRelation)
							.map(isWith -> isWith ? 1 : -1).orElse(0);
		}

		@Override
		public List<? extends SourceItem> getComponents()
		{
			return sourceItems;
		}

		@Override
		public Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> validate(DataStructureComponent<Measure, ?, ?> measure, CheckMode mode,
				Map<? extends ScalarValue<?, ?, ?>, ? extends ScalarValue<?, ?, ?>> values)
		{
//			LOGGER.trace("Start evaluating {}",
//					Utils.getStream(values.entrySet()).filter(byKey(k -> codeItem.equals(k.get()) || contains(STRINGDS.cast(k)))).collect(entriesToMap()));
//
//			Optional<NumberValue<?, ?, ?>> opLeft = Optional.ofNullable(values.get(new StringValue(codeItem))).filter(NumberValue.class::isInstance)
//					.map(NumberValue.class::cast);
//			LOGGER.trace("Left value is {}", opLeft.isPresent() ? "present" : "not present");
//
//			final boolean missingToNull = mode == NON_NULL || mode == PARTIAL_NULL || mode == PARTIAL_NULL;
//			final double leftValue = opLeft.map(NumberValue::doubleValue).orElse(missingToNull ? NaN : 0);
//
//			final AtomicBoolean allExistNotNull = new AtomicBoolean(opLeft.isPresent());
//			final AtomicBoolean oneNonZeroExists = new AtomicBoolean(opLeft.map(v -> v.doubleValue() != 0.0).orElse(FALSE));
//			final AtomicBoolean oneExists = new AtomicBoolean(opLeft.isPresent());
//
//			final DoubleValue imbalanceValue = new DoubleValue(leftValue - Utils.getStream(getComponents()).map(SourceItem::getId).map(StringValue::new)
//					.map(values::get).peek(v -> allExistNotNull.compareAndSet(true, v != null && !(v instanceof NullValue)))
//					.peek(v -> oneNonZeroExists.compareAndSet(false, v instanceof NumberValue && ((NumberValue<?, ?, ?>) v).doubleValue() != 0))
//					.peek(v -> oneExists.compareAndSet(false, v instanceof NumberValue))
//					.map(v -> v == null ? missingToNull ? NullValue.instance(UNKNOWNDS) : ZERO : v)
//					.mapToDouble(v -> v instanceof NumberValue ? ((NumberValue<?, ?, ?>) v).doubleValue() : NaN).sum());
//
//			LOGGER.trace("Evaluated imbalance: {}", imbalanceValue);
//
//			if (mode == NON_NULL && !allExistNotNull.get())
//			{
//				LOGGER.trace("NON_NULL: some values are NULL");
//				return emptyMap();
//			}
//			else if (mode == NON_ZERO && !oneNonZeroExists.get())
//			{
//				LOGGER.trace("NON_ZERO: all values are 0 or NULL");
//				return emptyMap();
//			}
//			else if ((mode == PARTIAL_NULL || mode == PARTIAL_ZERO) && !oneExists.get())
//			{
//				LOGGER.trace("PARTIAL: all values are 0 or NULL");
//				return emptyMap();
//			}
//
//			final ScalarValue<?, BooleanDomainSubset, BooleanDomain> test2 = opLeft.isPresent() ? relation.getAutoFunction().apply(imbalanceValue, ZERO)
//					: BooleanValue.FALSE;
//
//			BooleanValue test = (BooleanValue) test2;
//
//			LOGGER.trace("Check result is {}", test);
//
//			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> results = new HashMap<>();
//			results.put(measure, opLeft.map(ScalarValue.class::cast).orElse(NullValue.instance(measure.getDomain())));
//			results.put(ruleid, new StringValue(codeItem));
//			results.put(bool_var, test);
//			results.put(errorlevel, test.get() ? NullValue.instance(INTEGERDS) : INTEGERDS.cast(getErrorLevel()));
//			results.put(errorcode, test.get() ? NullValue.instance(STRINGDS) : STRINGDS.cast(getErrorCode()));
//			results.put(imbalance, imbalanceValue);
//
//			return results;
			
			throw new UnsupportedOperationException();
		}

		@Override
		public String toString()
		{
			return codeItem + " " + relation + " " + sourceItems.stream().map(Object::toString).collect(Collectors.joining(" ")) + " errorcode " + errorCode
					+ " errorlevel " + errorLevel;
		}
	}

	private final String item;
	private final List<RuleItem> rules;
	private final Map<String, String> conditions;

	public HierarchyImpl(String item, List<? extends RuleItem> rules, Map<String, String> conditions)
	{
		this.item = item;
		this.rules = new ArrayList<>(rules);
		this.conditions = conditions;
	}

	@Override
	public String getName()
	{
		return getItem();
	}

	@Override
	public List<RuleItem> getRuleItems()
	{
		return rules;
	}

	@Override
	public final VTLValueMetadata getMetadata()
	{
		return this;
	}

	public String getItem()
	{
		return item;
	}

	public String getCondition(String condition)
	{
		return conditions.get(condition);
	}
}
