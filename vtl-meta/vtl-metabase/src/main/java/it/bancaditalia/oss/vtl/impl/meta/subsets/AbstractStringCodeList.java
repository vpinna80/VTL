package it.bancaditalia.oss.vtl.impl.meta.subsets;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toSet;

import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.meta.subsets.AbstractStringCodeList.StringCodeItemImpl;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.StringCodeItem;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.util.Utils;

public abstract class AbstractStringCodeList implements StringEnumeratedDomainSubset<AbstractStringCodeList, StringCodeItemImpl, String>
{
	private static final long serialVersionUID = 1L;

	public class StringCodeItemImpl extends StringValue<StringCodeItemImpl, AbstractStringCodeList> implements StringCodeItem<StringCodeItemImpl, String, AbstractStringCodeList>
	{
		private static final long serialVersionUID = 1L;

		public StringCodeItemImpl(String value)
		{
			super(value, AbstractStringCodeList.this);
		}

		@Override
		public int compareTo(ScalarValue<?, ?, ?, ?> o)
		{
			return get().compareTo((String) STRINGDS.cast(o).get());
		}

		@Override
		public String toString()
		{
			return '"' + get() + '"';
		}
	}

	private final StringDomainSubset<?> parent;
	private final String name; 
	private final Function<Set<String>, AbstractStringCodeList> andThen;

	private int hashCode;

	public AbstractStringCodeList(StringDomainSubset<?> parent, String name, Function<Set<String>, AbstractStringCodeList> andThen)
	{
		this.name = name;
		this.parent = parent;
		this.andThen = andThen;
	}

	@Override
	public final String getName()
	{
		return name;
	}

	@Override
	public final StringDomainSubset<?> getParentDomain()
	{
		return parent;
	}

	@Override
	public final boolean isComparableWith(ValueDomain other)
	{
		return STRINGDS.isComparableWith(other);
	}

	public final int hashCode()
	{
		return hashCode;
	}

	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (obj instanceof AbstractStringCodeList)
			return false;
		AbstractStringCodeList other = (AbstractStringCodeList) obj;
		if (name == null)
		{
			if (other.name != null)
				return false;
		}
		else if (!name.equals(other.name))
			return false;

		return true;
	}

	@Override
	public StringCodeItemImpl cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value instanceof StringValue)
		{
			StringCodeItemImpl item = new StringCodeItemImpl((String) value.get());
			if (getCodeItems().contains(item))
				return item;
		}

		throw new VTLCastException(this, value);
	}

	@Override
	public ScalarValue<?, ?, AbstractStringCodeList, StringDomain> getDefaultValue()
	{
		return NullValue.instance(this);
	}

	@Override
	public AbstractStringCodeList trim()
	{
		return stringCodeListHelper("TRIM(" + getName() + ")", String::trim);
	}

	@Override
	public AbstractStringCodeList ltrim()
	{
		return stringCodeListHelper("LTRIM(" + getName() + ")", s -> s.replaceAll("^\\s+",""));
	}

	@Override
	public AbstractStringCodeList rtrim()
	{
		return stringCodeListHelper("RTRIM(" + getName() + ")", s -> s.replaceAll("\\s+$",""));
	}

	@Override
	public AbstractStringCodeList ucase()
	{
		return stringCodeListHelper("UCASE(" + getName() + ")", String::toUpperCase);
	}

	@Override
	public AbstractStringCodeList lcase()
	{
		return stringCodeListHelper("LCASE(" + getName() + ")", String::toLowerCase);
	}
	
	protected void setHashCode(int hashCode)
	{
		this.hashCode = hashCode;
	}

	protected AbstractStringCodeList stringCodeListHelper(String newName, UnaryOperator<String> mapper)
	{
		return Utils.getStream(getCodeItems()).map(ScalarValue::get).map(Object::toString).map(mapper).collect(collectingAndThen(toSet(), andThen));
	}
	
	@Override
	public String toString()
	{
		return name + ":" + parent;
	}
}
