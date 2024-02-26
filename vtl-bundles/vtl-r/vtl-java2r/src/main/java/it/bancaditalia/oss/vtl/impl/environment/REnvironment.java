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

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
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
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.util.Utils;

public class REnvironment implements Environment
{
	private final static Logger LOGGER = LoggerFactory.getLogger(REnvironment.class);;
	
	private final Rengine engine = RUtils.RENGINE;

	public Rengine getEngine()
	{
		return engine;
	}
	
	@Override
	public Optional<VTLValueMetadata> getValueMetadata(String name)
	{
		LOGGER.info("Searching for {} in R global environment...");
		if (reval("exists('???')", name).asBool().isTRUE())
		{
			if (reval("is.data.frame(`???`)", name).asBool().isTRUE()) 
			{
				LOGGER.info("Found a data.frame, constructing VTL metadata...");
				REXP data = reval("`???`[1, ]", name);
				RList dataFrame = data.asList();

				// manage measure and identifier attributes
				List<String> measures = new ArrayList<>();
				REXP measureAttrs = data.getAttribute("measures");
				if(measureAttrs != null) {
					measures = Arrays.asList(measureAttrs.asStringArray());
				}
				
				List<String> identifiers = new ArrayList<>();
				REXP idAttr = data.getAttribute("identifiers");
				if(idAttr != null) {
					if (reval("any(duplicated(`???`[, attr(`???`, 'identifiers')]))", name).asBool().isTRUE())
						throw new IllegalStateException("Found duplicated rows in data frame " + name);
					identifiers = Arrays.asList(idAttr.asStringArray());
				}
				
				DataStructureBuilder builder = new DataStructureBuilder();
				for (String key: dataFrame.keys())
				{
					REXP columnData = dataFrame.at(key);

					Class<? extends Component> type;
					if (measures.contains(key))
						type = Measure.class;
					else if (identifiers.contains(key))
						type = Identifier.class;
					else
						type = Attribute.class;

					ValueDomainSubset<?, ?> domain;
					switch (columnData.getType())
					{
						case XT_DOUBLE: case XT_ARRAY_DOUBLE:
							domain = NUMBERDS;
							break;
						case XT_INT: case XT_ARRAY_INT:
							domain = INTEGERDS;
							break;
						case XT_STR: case XT_ARRAY_STR:
							domain = STRINGDS;
							break;
						case XT_BOOL: case XT_ARRAY_BOOL: case XT_ARRAY_BOOL_INT:
							domain = BOOLEANDS;
							break;
						default:
							throw new UnsupportedOperationException(
									"Unrecognized data.frame column type in " + name + ": " + key + "(" + REXP.xtName(columnData.getType()) + ")");
					}
					
					builder.addComponent(DataStructureComponentImpl.of(key, type, domain));
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
					case XT_STR:
						return Optional.of(STRING);
					case XT_ARRAY_DOUBLE:
						return Optional.of(NUMBER);
					case XT_ARRAY_INT:
						return Optional.of(INTEGER);
					case XT_ARRAY_BOOL: case XT_ARRAY_BOOL_INT:
						return Optional.of(BOOLEAN);
					default:
						throw new UnsupportedOperationException(
								"Unrecognized scalar value " + name + ": " + REXP.xtName(data.getType()) + ")");
				}
			}
		}

		return Optional.empty();
	}
	
	@Override
	public Optional<VTLValue> getValue(String name)
	{
		if (reval("exists('???')", name).asBool().isTRUE())
		{
			VTLValue result;

			if (reval("is.data.frame(`???`)", name).asBool().isTRUE()) {
				DataSetMetadata metadata = ConfigurationManagerFactory.getInstance().getMetadataRepository().getStructure(name);
				result = parseDataFrame(name, metadata);
				return Optional.of(result);
			}
			else if (reval("is.integer(`???`) || is.numeric(`???`) || is.character(`???`)", name).asBool().isTRUE())
			{
				REXP data = reval("`???`", name);
				switch (data.getType())
				{
					case XT_STR:
						result = StringValue.of(data.asString());
						break;
					case XT_ARRAY_DOUBLE:
						result = NumberValueImpl.createNumberValue(data.asDoubleArray()[0]);
						break;
					case XT_ARRAY_INT:
						result = IntegerValue.of((long) data.asIntArray()[0]);
						break;
					case XT_ARRAY_BOOL: case XT_ARRAY_BOOL_INT:
						result = BooleanValue.of(data.asBool().isTRUE());
					break;
					default:
						throw new IllegalStateException("Node: " + name + " of scalar type: " + REXP.xtName(data.getType()) + ". This is not supported.");
				}
				return Optional.of(result);
			}
		}

		return Optional.empty();
	}

	private DataSet parseDataFrame(String name, DataSetMetadata metadata)
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
			ValueDomainSubset<?, ?> domain;
			switch (columnData.getType())
			{
				case XT_ARRAY_DOUBLE:
					if(dateColumns.contains(key))
					{
						domain = DATEDS;
						// NAs are mapped to something that returns true to is.NaN()
						values = Utils.getStream(columnData.asDoubleArray()).mapToObj(val -> (ScalarValue<?, ?, ?, ?>) (Double.isNaN(val) ? NullValue.instance(DATEDS) :  DateValue.of(LocalDate.of(1970, 1, 1).plus((long) val, DAYS) )));   
					}
					else
					{
						// NAs are mapped to something that retrns true to is.NaN()
						domain = NUMBERDS;
						values = Utils.getStream(columnData.asDoubleArray()).mapToObj(NumberValueImpl::createNumberValue);
					}
					break;
				case XT_ARRAY_INT:
					// NAs are mapped to Integer.MIN_VALUE
					domain = INTEGERDS;
					values = Utils.getStream(columnData.asIntArray()).asLongStream().mapToObj(IntegerValue::of);
					break;
				case XT_ARRAY_STR:
					domain = STRINGDS;
					values = Utils.getStream(columnData.asStringArray()).map(StringValue::of);
					break;
				case XT_ARRAY_BOOL: case XT_ARRAY_BOOL_INT:
					domain = BOOLEANDS;
					values = Utils.getStream(columnData.asIntArray()).mapToObj(val -> (ScalarValue<?, ?, ?, ?>) ((val != 1 && val != 0) ? NullValue.instance(BOOLEANDS) : BooleanValue.of(val == 1)));
					break;
				default:
					throw new IllegalStateException(
							"In node: " + name + " there is a column (" + key + ") of type " + REXP.xtName(columnData.getType()) + ". This is not supported.");
			}
			
			final DataStructureComponent<?, ?, ?> comp;
			if (metadata != null)
			{
				comp = metadata.getComponent('\'' + key + '\'').orElseThrow(() -> {
					return new VTLMissingComponentsException(metadata, key);
				});
				if (comp.getVariable().getDomain() instanceof TimeDomainSubset)
					values = values.map(REnvironment.toTime(comp));
				else
					values = values.map(v -> comp.getVariable().getDomain().cast(v));
			}
			else
			{
				// manage measure and identifier attributes
				List<String> identifiers = new ArrayList<>();
				List<String> measures = new ArrayList<>();
				REXP measureAttr = data.getAttribute("measures");
				if (measureAttr != null && (measureAttr.getType() == XT_ARRAY_STR || measureAttr.getType() == XT_STR))
					measures = Arrays.asList(measureAttr.asStringArray());
				REXP idAttr = data.getAttribute("identifiers");
				if (idAttr != null && (idAttr.getType() == XT_ARRAY_STR || idAttr.getType() == XT_STR))
					identifiers = Arrays.asList(idAttr.asStringArray());

				Class<? extends Component> type;
				if (measures.contains(key))
					type = Measure.class;
				else if (identifiers.contains(key))
					type = Identifier.class;
				else
					type = Attribute.class;
				comp = DataStructureComponentImpl.of(key, type, domain);
			}
			
			dataContainer.put(comp, values.toArray(ScalarValue<?, ?, ?, ?>[]::new));
		}
		
		if (metadata != null)
		{
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
		}
		
		return new ColumnarDataSet(name, dataContainer);
	}

	@Override
	public boolean contains(String alias)
	{
		return reval("exists('???')", alias).asBool().isTRUE();
	}
	
	private REXP reval(String format, String name) 
	{
		return getEngine().eval(format.replace("???", name));
	}

	private static UnaryOperator<ScalarValue<?, ?, ?, ?>> toTime(DataStructureComponent<?, ?, ?> comp)
	{
		return value -> {
			if (value instanceof NullValue)
				return NullValue.instanceFrom(comp);
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
