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
package it.bancaditalia.oss.vtl.impl.environment;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RList;
import org.rosuda.JRI.Rengine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.impl.environment.dataset.ColumnarDataSet;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.util.Utils;

public class REnvironment implements Environment
{
	private final static Logger LOGGER = LoggerFactory.getLogger(REnvironment.class);
	private final Map<String, VTLValue>	values	= new HashMap<>();
	private final Rengine engine = new Rengine();

	public Rengine getEngine()
	{
		return engine;
	}
	
	@Override
	public Optional<VTLValueMetadata> getValueMetadata(String name)
	{
		if (values.containsKey(name))
			return Optional.of(values.get(name).getMetadata());

		if (getEngine().eval("exists('" + name + "')").asBool().isTRUE())
		{
			if (getEngine().eval("is.data.frame(" + name + ")").asBool().isTRUE()) 
			{
				REXP data = getEngine().eval(name + "[1,]");
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
					if (getEngine().eval("any(duplicated(" + name + "[,attr(" + name + ", 'identifiers')]))").asBool().isTRUE())
						throw new IllegalStateException("Found duplicated rows in data frame " + name);
					identifiers = Arrays.asList(idAttr.asStringArray());
				}
				
				DataStructureBuilder builder = new DataStructureBuilder();
				for (String key: dataFrame.keys())
				{
					REXP columnData = dataFrame.at(key);

					Class<? extends ComponentRole> type;
					if (measures.contains(key))
						type = Measure.class;
					else if (identifiers.contains(key))
						type = Identifier.class;
					else
						type = Attribute.class;

					ValueDomainSubset<?> domain;
					switch (columnData.getType())
					{
						case REXP.XT_DOUBLE: case REXP.XT_ARRAY_DOUBLE:
							domain = NUMBERDS;
							break;
						case REXP.XT_INT: case REXP.XT_ARRAY_INT:
							domain = INTEGERDS;
							break;
						case REXP.XT_STR: case REXP.XT_ARRAY_STR:
							domain = STRINGDS;
							break;
						case REXP.XT_BOOL: case REXP.XT_ARRAY_BOOL: case REXP.XT_ARRAY_BOOL_INT:
							domain = BOOLEANDS;
							break;
						default:
							throw new UnsupportedOperationException(
									"Unrecognized data.frame column type in " + name + ": " + key + "(" + REXP.xtName(columnData.getType()) + ")");
					}
					
					builder.addComponent(key, type, domain);
				}
				
				return Optional.of(builder.build());
			}
			else if (getEngine().eval("is.integer(" + name + ") || is.numeric(" + name + ") || is.character(" + name + ") || is.logical(" + name + ")")
					.asBool().isTRUE())
			{
				REXP data = getEngine().eval(name);
				switch (data.getType())
				{
					case REXP.XT_STR:
						return Optional.of(STRING);
					case REXP.XT_ARRAY_DOUBLE:
						return Optional.of(NUMBER);
					case REXP.XT_ARRAY_INT:
						return Optional.of(INTEGER);
					case REXP.XT_ARRAY_BOOL: case REXP.XT_ARRAY_BOOL_INT:
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
		if (values.containsKey(name))
			return Optional.of(values.get(name));

		if (getEngine().eval("exists('" + name + "')").asBool().isTRUE())
		{
			VTLValue result;

			if (getEngine().eval("is.data.frame(" + name + ")").asBool().isTRUE()) {
				result = parseDataFrame(name);
				values.put(name, result);
				return Optional.of(result);
			}
			else if (getEngine().eval("is.integer(" + name + ") || is.numeric(" + name + ") || is.character(" + name +
					")").asBool().isTRUE())
			{
				REXP data = getEngine().eval(name);
				switch (data.getType())
				{
					case REXP.XT_STR:
						result = new StringValue(data.asString());
						break;
					case REXP.XT_ARRAY_DOUBLE:
						result = new DoubleValue(data.asDoubleArray()[0]);
						break;
					case REXP.XT_ARRAY_INT:
						result = new IntegerValue((long) data.asIntArray()[0]);
						break;
					case REXP.XT_ARRAY_BOOL: case REXP.XT_ARRAY_BOOL_INT:
						result = data.asBool().isNA() ? NullValue.instance(BOOLEANDS) : BooleanValue.of(data.asBool().isTRUE());
						break;
					default:
						throw new IllegalStateException("Node: " + name + " of scalar type: " + REXP.xtName(data.getType()) + ". This is not supported.");
				}
				values.put(name, result);
				return Optional.of(result);
			}
		}

		return Optional.empty();
	}

	private DataSet parseDataFrame(String name)
	{
		List<String> identifiers = new ArrayList<>();
		List<String> measures = new ArrayList<>();

		// transform factors into strings
		getEngine().eval("if(any(sapply(" + name + ", is.factor))) " + name + "[which(sapply(" + name + ", is.factor))] <- sapply(" + name + "[which(sapply(" + name
				+ ", is.factor))], as.character)");

		LOGGER.info("Migrating dataset {} from R", name);
		
		REXP data = getEngine().eval(name);
		RList dataFrame = data.asList();

		// manage measure and identifier attributes
		REXP measureAttr = data.getAttribute("measures");
		if (measureAttr != null && (measureAttr.getType() == REXP.XT_ARRAY_STR || measureAttr.getType() == REXP.XT_STR))
			measures = Arrays.asList(measureAttr.asStringArray());

		REXP idAttr = data.getAttribute("identifiers");
		if (idAttr != null && (idAttr.getType() == REXP.XT_ARRAY_STR || idAttr.getType() == REXP.XT_STR))
			identifiers = Arrays.asList(idAttr.asStringArray());

		Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>[]> dataContainer = new HashMap<>();
		// get column data
		for (String key: dataFrame.keys())
		{
			REXP columnData = dataFrame.at(key);

			Class<? extends ComponentRole> type;
			if (measures.contains(key))
				type = Measure.class;
			else if (identifiers.contains(key))
				type = Identifier.class;
			else
				type = Attribute.class;

			Stream<? extends ScalarValue<?, ?, ?>> values;
			ValueDomainSubset<?> domain;
			switch (columnData.getType())
			{
				case REXP.XT_ARRAY_DOUBLE:
					domain = NUMBERDS;
					values = Utils.getStream(columnData.asDoubleArray()).mapToObj(DoubleValue::new);
					break;
				case REXP.XT_ARRAY_INT:
					domain = INTEGERDS;
					values = Utils.getStream(columnData.asIntArray()).asLongStream().mapToObj(IntegerValue::new);
					break;
				case REXP.XT_ARRAY_STR:
					domain = STRINGDS;
					values = Utils.getStream(columnData.asStringArray()).map(StringValue::new);
					break;
				case REXP.XT_ARRAY_BOOL: case REXP.XT_ARRAY_BOOL_INT:
					domain = BOOLEANDS;
					values = Utils.getStream(columnData.asIntArray()).mapToObj(val -> (ScalarValue<?, ?, ?>) (val == 2 ? NullValue.instance(BOOLEANDS) : BooleanValue.of(val == 1)));
					break;
				default:
					throw new IllegalStateException(
							"In node: " + name + " there is a column (" + key + ") of type " + REXP.xtName(columnData.getType()) + ". This is not supported.");
			}
			
			dataContainer.put(new DataStructureComponentImpl<>(key, type, domain), values.toArray(ScalarValue<?, ?, ?>[]::new));
		}
		
		return new ColumnarDataSet(dataContainer);
	}

	@Override
	public boolean contains(String id)
	{
		return values.containsKey(id);
	}
}
