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
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.Utils;

public class REnvironment implements Environment
{
	private final static Logger LOGGER = LoggerFactory.getLogger(REnvironment.class);
	
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
				
				DataSetStructureBuilder builder = new DataSetStructureBuilder();
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
 					
					builder.addComponent(DataSetComponentImpl.of(VTLAliasImpl.of(true, key), domain, role));
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
				Optional<VTLValueMetadata> maybeMeta = repo.getMetadata(name).or(() -> getValueMetadata(name));
				if (maybeMeta.isEmpty())
					return Optional.empty();
				
				DataSetStructure metadata = (DataSetStructure) maybeMeta.get();
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

	private DataSet parseDataFrame(DataSetStructure metadata, VTLAlias name)
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

		Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>[]> dataContainer = new HashMap<>();
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
			
			DataSetComponent<?, ?, ?> comp = metadata.getComponent(VTLAliasImpl.of(true, key)).orElse(null);
			if (comp == null)
				continue;
			if (comp.getDomain() instanceof TimeDomainSubset)
				values = values.map(REnvironment.toTime(comp));
			else
				values = values.map(v -> comp.getDomain().cast(v));
			
			dataContainer.put(comp, values.toArray(ScalarValue<?, ?, ?, ?>[]::new));
		}
		
		for (DataSetComponent<Attribute, ?, ?> comp: metadata.getComponents(Attribute.class))
			if (!dataContainer.containsKey(comp))
			{
				ScalarValue<?, ?, ?, ?>[] array = new ScalarValue<?, ?, ?, ?>[len];
				Arrays.fill(array, NullValue.instanceFrom(comp));
				dataContainer.put(comp, array);
			}
		
		Set<DataSetComponent<?, ?, ?>> missing = new HashSet<>(metadata);
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

	private static UnaryOperator<ScalarValue<?, ?, ?, ?>> toTime(DataSetComponent<?, ?, ?> comp)
	{
		return value -> {
			if (value.isNull())
				return NullValue.instanceFrom(comp);
			else if (value instanceof DateValue)
			{
				return value;
			}
			else if (value instanceof NumberValue)
			{
				int year = ((Number) value.get()).intValue();
				return TimePeriodValue.of(new YearPeriodHolder(Year.of(year)));
			}
			else if (value instanceof StringValue)
			{
				String dt = value.get().toString(); 
				if (dt.matches("^\\d{4}-\\d{2}$"))
					return TimePeriodValue.of(new MonthPeriodHolder(YearMonth.parse(dt)));
				else if (dt.matches("^\\d{4}-\\d{2}-\\d{2}$"))
					return new DateValue<>(LocalDate.parse(dt), DATEDS);
				else
					throw new UnsupportedOperationException("Date format not supported: " + dt);
			}
			else
				throw new VTLCastException(comp.getDomain(), value);
		};
	}
}
