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
package it.bancaditalia.oss.vtl.impl.environment;

import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIME_PERIODDS;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.rosuda.JRI.REXP.XT_ARRAY_BOOL;
import static org.rosuda.JRI.REXP.XT_ARRAY_BOOL_INT;
import static org.rosuda.JRI.REXP.XT_ARRAY_DOUBLE;
import static org.rosuda.JRI.REXP.XT_ARRAY_INT;
import static org.rosuda.JRI.REXP.XT_ARRAY_STR;
import static org.rosuda.JRI.REXP.XT_BOOL;
import static org.rosuda.JRI.REXP.XT_DOUBLE;
import static org.rosuda.JRI.REXP.XT_INT;
import static org.rosuda.JRI.REXP.XT_STR;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.Year;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RList;
import org.rosuda.JRI.Rengine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.environment.dataset.ColumnarDataSet;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.data.Variable.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.Utils;

public class REnvironment implements Environment
{
	private final static Logger LOGGER = LoggerFactory.getLogger(REnvironment.class);
	
	private static class RVar<S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements Variable<S, D>, Serializable
	{
		private static final long serialVersionUID = 1L;

		private final VTLAlias name;
		private final S domain;
		
		private transient int hashCode;

		@SuppressWarnings("unchecked")
		public RVar(VTLAlias name, ValueDomainSubset<?, ?> domain)
		{
			this.name = name;
			this.domain = (S) domain;
		}
		
		@Override
		public VTLAlias getAlias()
		{
			return name;
		}

		@Override
		public S getDomain()
		{
			return domain;
		}
		
		@Override
		public <R1 extends Component> DataStructureComponent<R1, S, D> as(Class<R1> role)
		{
			return new DataStructureComponentImpl<>(role, this);
		}

		@Override
		public int hashCode()
		{
			return hashCode == 0 ? hashCode = hashCodeInit() : hashCode;
		}

		public int hashCodeInit()
		{
			final int prime = 31;
			int result = 1;
			result = prime * result + domain.hashCode();
			result = prime * result + name.hashCode();
			hashCode = result;

			return result;
		}

		@Override
		public boolean equals(Object obj)
		{
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (obj instanceof Variable)
			{
				Variable<?, ?> var = (Variable<?, ?>) obj;
				return name.equals(var.getAlias()) && domain.equals(var.getDomain());
			}
			
			return false;
		}
	}
	
	private final Rengine engine = RUtils.RENGINE;

	public Rengine getEngine()
	{
		return engine;
	}
	
	@Override
	public Optional<VTLValueMetadata> getValueMetadata(VTLAlias name)
	{
		LOGGER.info("Searching for {} in R global environment...");
		if (reval("exists('???')", name).asBool().isTRUE())
			if (reval("is.data.frame(`???`)", name).asBool().isTRUE()) 
			{
				LOGGER.info("Found a data.frame, constructing VTL metadata...");
				REXP data = reval("`???`[1, ]", name);
				RList dataFrame = data.asList();

				// manage measure and identifier attributes
				List<String> measures = new ArrayList<>();
				REXP attributeMeas = data.getAttribute("measures");
				if(attributeMeas != null) 
					measures = Arrays.asList(attributeMeas.asStringArray());
				
				List<String> identifiers = new ArrayList<>();
				REXP idAttr = data.getAttribute("identifiers");
				if(idAttr != null)
				{
					if (reval("any(duplicated(`???`[, attr(`???`, 'identifiers')]))", name).asBool().isTRUE())
						throw new IllegalStateException("Found duplicated rows in data frame " + name);
					identifiers = Arrays.asList(idAttr.asStringArray());
				}
				
				DataStructureBuilder builder = new DataStructureBuilder();
				for (String key: dataFrame.keys())
				{
					REXP columnData = dataFrame.at(key);

					Class<? extends Component> role;
					if (measures.contains(key))
						role = Measure.class;
					else if (identifiers.contains(key))
						role = Identifier.class;
					else
						role = Attribute.class;

					ValueDomainSubset<?, ?> domain;
					Optional<String[]> classes = Optional.ofNullable(columnData.getAttribute("class")).map(REXP::asStringArray);
					switch (columnData.getType())
					{
						case XT_DOUBLE: case XT_ARRAY_DOUBLE:                     
							domain = classes.isPresent() && Arrays.asList(classes.get()).contains("Date") ? DATEDS : NUMBERDS; break;
						case XT_INT: case XT_ARRAY_INT:                           domain = INTEGERDS; break;
						case XT_STR: case XT_ARRAY_STR:                           domain = STRINGDS; break;
						case XT_BOOL: case XT_ARRAY_BOOL: case XT_ARRAY_BOOL_INT: domain = BOOLEANDS; break;
						default: throw new UnsupportedOperationException(
							"Unrecognized data.frame column type in " + name + ": " + key + "(" + REXP.xtName(columnData.getType()) + ")");
					}
 					
					builder.addComponent(new RVar<>(VTLAliasImpl.of(true, key), domain).as(role));
				}
				
				LOGGER.info("VTL metadata for {} completed.", name);
				return Optional.of(builder.build());
			}
			else if (reval("is.integer(`???`) || is.numeric(`???`) || is.character(`???`) || is.logical(`???`)", name).asBool().isTRUE())
			{
				LOGGER.info("Found a scalar value.");
				REXP data = reval("`???`", name);
				switch (data.getType())
				{
					case XT_STR:            return Optional.of(STRING);
					case XT_ARRAY_DOUBLE:   return Optional.of(NUMBER);
					case XT_ARRAY_INT:      return Optional.of(INTEGER);
					case XT_ARRAY_BOOL:     return Optional.of(BOOLEAN);
					case XT_ARRAY_BOOL_INT: return Optional.of(BOOLEAN);
					default:
						throw new UnsupportedOperationException("Unrecognized scalar value " + name + ": " + REXP.xtName(data.getType()) + ")");
				}
			}

		return Optional.empty();
	}
	
	@Override
	public Optional<VTLValue> getValue(MetadataRepository repo, VTLAlias name)
	{
		if (reval("exists('???')", name).asBool().isTRUE())
			if (reval("is.data.frame(`???`)", name).asBool().isTRUE())
			{
				Optional<VTLValueMetadata> maybeMeta = getValueMetadata(name).or(() -> repo.getStructure(name));
				if (maybeMeta.isEmpty())
					return Optional.empty();
				
				DataSetMetadata metadata = (DataSetMetadata) maybeMeta.get();
				return Optional.of(parseDataFrame(metadata, name));
			}
			else if (reval("is.integer(`???`) || is.numeric(`???`) || is.character(`???`)", name).asBool().isTRUE())
			{
				REXP data = reval("`???`", name);
				VTLValue result;
				switch (data.getType())
				{
					case XT_STR: result = StringValue.of(data.asString()); break;
					case XT_ARRAY_DOUBLE: result = createNumberValue(data.asDoubleArray()[0]); break;
					case XT_ARRAY_INT: result = IntegerValue.of((long) data.asIntArray()[0]); break;
					case XT_ARRAY_BOOL: result = BooleanValue.of(data.asBool().isTRUE()); break;
					case XT_ARRAY_BOOL_INT: result = BooleanValue.of(data.asBool().isTRUE()); break;
					default:
						throw new IllegalStateException("Node: " + name + " of scalar type: " + REXP.xtName(data.getType()) + ". This is not supported.");
				}
				return Optional.of(result);
			}

		return Optional.empty();
	}

	private DataSet parseDataFrame(DataSetMetadata metadata, VTLAlias name)
	{
		List<String> dateColumns = new ArrayList<>();
 
		// transform factors into strings
		reval("if(any(sapply(`???`, is.factor))) `???`[which(sapply(`???`, is.factor))] <- sapply(`???`[which(sapply(`???`, is.factor))], as.character)", name);

		// Determine if R data.frame column values have class Date
		REXP dates = reval("names(which(sapply(names(`???`), function(x, y) class(y[,x]), y = `???`) == 'Date'))", name);
		if(dates != null)
			dateColumns = Arrays.asList(dates.asStringArray());
		
		REXP data = reval("`???`", name);
		RList dataFrame = data.asList();
		int len = reval("nrow(`???`)", name).asInt();

		Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>[]> dataContainer = new HashMap<>();
		// get column data
		for (String key: dataFrame.keys())
		{
			REXP columnData = dataFrame.at(key);
			Stream<? extends ScalarValue<?, ?, ?, ?>> values;
			switch (columnData.getType())
			{
				case XT_ARRAY_DOUBLE:
					if(dateColumns.contains(key))
						values = Utils.getStream(columnData.asDoubleArray()).mapToObj(val -> (ScalarValue<?, ?, ?, ?>) (Double.isNaN(val) ? NullValue.instance(DATEDS) :  DateValue.of(LocalDate.of(1970, 1, 1).plus((long) val, DAYS) )));   
					else
						values = Utils.getStream(columnData.asDoubleArray()).mapToObj(NumberValueImpl::createNumberValue);
					break;
				case XT_ARRAY_INT:
					// NAs are mapped to Integer.MIN_VALUE
					values = Utils.getStream(columnData.asIntArray()).asLongStream().mapToObj(IntegerValue::of);
					break;
				case XT_ARRAY_STR:
					values = Utils.getStream(columnData.asStringArray()).map(StringValue::of);
					break;
				case XT_ARRAY_BOOL: case XT_ARRAY_BOOL_INT:
					values = Utils.getStream(columnData.asIntArray()).mapToObj(val -> (ScalarValue<?, ?, ?, ?>) ((val != 1 && val != 0) ? NullValue.instance(BOOLEANDS) : BooleanValue.of(val == 1)));
					break;
				default:
					throw new IllegalStateException(
							"In node: " + name + " there is a column (" + key + ") of type " + REXP.xtName(columnData.getType()) + ". This is not supported.");
			}
			
			DataStructureComponent<?, ?, ?> comp = metadata.getComponent(VTLAliasImpl.of(true, key)).orElse(null);
			if (comp == null)
				continue;
			if (comp.getVariable().getDomain() instanceof TimeDomainSubset)
				values = values.map(REnvironment.toTime(comp));
			else
				values = values.map(v -> comp.getVariable().getDomain().cast(v));
			
			dataContainer.put(comp, values.toArray(ScalarValue<?, ?, ?, ?>[]::new));
		}
		
		for (DataStructureComponent<Attribute, ?, ?> comp: metadata.getComponents(Attribute.class))
			if (!dataContainer.containsKey(comp))
			{
				ScalarValue<?, ?, ?, ?>[] array = new ScalarValue<?, ?, ?, ?>[len];
				Arrays.fill(array, NullValue.instanceFrom(comp));
				dataContainer.put(comp, array);
			}
		
		Set<DataStructureComponent<?, ?, ?>> missing = new HashSet<>(metadata);
		missing.removeAll(dataContainer.keySet());
		if (missing.size() > 0)
			throw new VTLMissingComponentsException(metadata, missing);
		
		return new ColumnarDataSet(name, dataContainer);
	}

	@Override
	public boolean contains(VTLAlias alias)
	{
		return reval("exists('???')", alias).asBool().isTRUE();
	}
	
	private REXP reval(String format, VTLAlias name) 
	{
		return getEngine().eval(format.replace("???", name.getName()));
	}

	private static UnaryOperator<ScalarValue<?, ?, ?, ?>> toTime(DataStructureComponent<?, ?, ?> comp)
	{
		return value -> {
			if (value instanceof NullValue)
				return NullValue.instanceFrom(comp);
			else if (value instanceof DateValue)
			{
				return value;
			}
			else if (value instanceof NumberValue)
			{
				int year = ((Number) value.get()).intValue();
				return new TimePeriodValue<>(new YearPeriodHolder(Year.of(year)), TIME_PERIODDS);
			}
			else if (value instanceof StringValue)
			{
				String dt = value.get().toString(); 
				if (dt.matches("^\\d{4}-\\d{2}$"))
					return new TimePeriodValue<>(new MonthPeriodHolder(YearMonth.parse(dt)), TIME_PERIODDS);
				else if (dt.matches("^\\d{4}-\\d{2}-\\d{2}$"))
					return new DateValue<>(LocalDate.parse(dt), DATEDS);
				else
					throw new UnsupportedOperationException("Date format not supported: " + dt);
			}
			else
				throw new VTLCastException(comp.getVariable().getDomain(), value);
		};
	}
}
