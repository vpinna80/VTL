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
package it.bancaditalia.oss.vtl.impl.engine.mapping;

import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.net.URL;
import java.security.InvalidParameterException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import javax.xml.transform.stream.StreamSource;

import org.antlr.v4.runtime.ParserRuleContext;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.grammar.Vtl;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Aliasparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Check;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Context;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Customparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Exprparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Listparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Mapping;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Nestedparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Nonnullparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.ObjectFactory;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Param;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Parserconfig;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Stringparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Tokenmapping;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Tokenscheck;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Tokenset;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Tokensetparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Valueparam;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;

public class MappingTest
{
	private static final Logger LOGGER = LoggerFactory.getLogger(MappingTest.class);
	private static final String MAPPING_FILENAME = OpsFactory.class.getName().replaceAll("\\.", "/") + ".xml";
	
	@Test
	public void mappingTest() throws IOException, JAXBException, ClassNotFoundException, NoSuchMethodException, SecurityException
	{
		JAXBContext jc = JAXBContext.newInstance(ObjectFactory.class);
		Enumeration<URL> files = Thread.currentThread().getContextClassLoader().getResources(MAPPING_FILENAME);
		if (!files.hasMoreElements())
		{
			IllegalStateException ex = new IllegalStateException("Cannot find VTL mapping file, " + MAPPING_FILENAME);
			LOGGER.error("Cannot find any mapping files. Forgot to add an implementation to your classpath?");
			throw ex;
		}

		boolean first = true;
		URL file = null;
		while (files.hasMoreElements())
			if (first)
			{
				file = files.nextElement();
				first = false;
				LOGGER.info("Using VTL configuration file: {}", file);
			}
			else
			{
				files.nextElement();
				LOGGER.warn("Ignored additional VTL configuration file: {}", file);
				if (!files.hasMoreElements())
					LOGGER.warn("Multiple configurations detected: you may have multiple implementations in your classpath!");
			}
		
		if (file == null)
			throw new FileNotFoundException("VTL mapping configuration file not found in classpath.");

		StreamSource xmlConfig = new StreamSource(file.openStream());
		Parserconfig config = jc.createUnmarshaller().unmarshal(xmlConfig, Parserconfig.class).getValue();
		String packageName = config.getPackage();

		Set<Class<?>> recursive = new HashSet<>();
		for (Context context : config.getRecursivecontexts().getContext())
		{
			Class<?> recClass = Class.forName("it.bancaditalia.oss.vtl.grammar.Vtl$" + context.getName(), true, Thread.currentThread().getContextClassLoader());
			recursive.add(recClass);
		}

		Map<String, Entry<Class<?>, Map<String, String>>> tokensets = new HashMap<>();
		for (Tokenset tokenset : config.getTokenset())
		{
			Class<?> tokenClass = Class.forName(tokenset.getClazz(), true, Thread.currentThread().getContextClassLoader());
			Map<String, String> mappings = new HashMap<>();
			for (Tokenmapping mapping: tokenset.getTokenmapping())
				mappings.put(mapping.getName(), mapping.getValue());
			tokensets.put(tokenset.getName(), new SimpleEntry<>(tokenClass, mappings));
		}

		Map<String, Set<String>> mappings = new HashMap<>();
		for (Mapping mapping : config.getMapping())
			for (String from: mapping.getFrom())
			{
				HashSet<String> tokens = new HashSet<>();
				
				Check check = mapping.getTokensOrContextOrNested();
				if (check instanceof Tokenscheck)
					tokens.addAll(((Tokenscheck) check).getValue());
				else if (check != null)
					throw new UnsupportedOperationException(check.getClass().getSimpleName() + " in " + from);
				
				if (mappings.containsKey(from))
					if (mappings.get(from).isEmpty())
					{
						assertNotNull(check, "Duplicated mapping " + from);
						mappings.put(from, emptySet());
					}
					else
					{
						Set<String> usedTokens = mappings.get(from);
						for (String token: tokens)
						{
							assertFalse(usedTokens.contains(token), "Duplicate check token for mapping " + from);
							usedTokens.add(token);
						}
					}
				else if (check == null)
					mappings.put(from, emptySet());
				else
					mappings.put(from, tokens);
				
				Class<?> fromClass = Class.forName("it.bancaditalia.oss.vtl.grammar.Vtl$" + from + "Context", true, Thread.currentThread().getContextClassLoader());
				Class<?> toClass = Class.forName(packageName + "." + mapping.getTo(), true, Thread.currentThread().getContextClassLoader());
				
				requireNonNull(fromClass, "from class missing in mapping: " + from);
				System.out.println("Checking mapping from " + fromClass.getSimpleName() + " to " + toClass.getSimpleName());
				checkParams(0, fromClass, tokensets, mapping.getParams().getNullparamOrAliasparamOrStringparam());
			}
	}

	private void checkParams(int level, Class<?> fromClass, Map<String, Entry<Class<?>, Map<String, String>>> tokensets, List<? extends Param> params) throws ClassNotFoundException, NoSuchMethodException, SecurityException
	{
		for (int i = 0; i < params.size(); i++)
			if (params.get(i) instanceof Nonnullparam)
				checkParam(level + 1, i, (Nonnullparam) params.get(i), fromClass, tokensets);
	}

	private void checkParam(int level, int index, Nonnullparam param, Class<?> fromClass, Map<String, Entry<Class<?>, Map<String, String>>> tokensets) throws ClassNotFoundException, NoSuchMethodException, SecurityException
	{
		String inClass = " in class " + fromClass.getSimpleName();
		String tabs = "    ".repeat(level);
		
		if (param instanceof Tokensetparam)
		{
			String tokenset = ((Tokensetparam) param).getTokenset();
			System.out.println(tabs + "Checking tokenset " + tokenset + inClass);
			assertNotNull(tokenset, "tokenset for tokensetparam in " + fromClass.getSimpleName());
			assertTrue(tokensets.containsKey(tokenset), "Undefined tokenset + " + tokenset);
			if (param.getName() != null)
				checkMember(param, fromClass);
		}
		else if (param instanceof Listparam)
		{
			Listparam lParam = (Listparam) param;
			String lName = lParam.getName();
			System.out.println(tabs + "Checking listParam " + lName + inClass);

			Member member = checkMember(lParam, fromClass);
			Class<?> innerClass = null;
			if (member instanceof Method)
			{
				assertEquals(List.class, ((Method) member).getReturnType(), "List<?> " + lName + "() not found");
				innerClass = member.getDeclaringClass().getMethod(member.getName(), int.class).getReturnType();
			}
			else
			{
				ParameterizedType genericType = (ParameterizedType) ((Field) member).getGenericType();
				assertEquals(List.class, genericType.getRawType(), "List<?> " + lName + "() not found");
				innerClass = (Class<?>) genericType.getActualTypeArguments()[0];
			}

			Nonnullparam itemParam = lParam.getStringparamOrAliasparamOrExprparam();
			checkParam(level + 1, index, itemParam, innerClass, tokensets);
		}
		else if (param instanceof Nestedparam)
		{
			Nestedparam nParam = (Nestedparam) param;
			System.out.print(tabs + "Checking nestedParam " + nParam.getName() + inClass);
			
			Member member = checkMember(nParam, fromClass);
			Class<?> innerClass = fromClass;
			if (member != null)
				innerClass = member instanceof Field ? ((Field) member).getType() : ((Method) member).getReturnType();
			
			System.out.println(": is " + innerClass.getSimpleName());
			assertNotNull(innerClass);
			checkParams(level, innerClass, tokensets, nParam.getNullparamOrAliasparamOrStringparam());
		}
		else if (param instanceof Customparam)
		{
			Customparam cParam = (Customparam) param;
			System.out.print(tabs + "Checking customParam " + cParam.getName() + inClass);
			Class.forName(cParam.getClazz(), true, Thread.currentThread().getContextClassLoader());
			
			Member member = checkMember(cParam, fromClass);
			Class<?> innerClass = fromClass;
			if (member != null)
				innerClass = member instanceof Field ? ((Field) member).getType() : ((Method) member).getReturnType();

			System.out.println(": is " + innerClass.getSimpleName());
			assertNotNull(innerClass);
			checkParams(level, innerClass, tokensets, cParam.getStringparamOrAliasparamOrExprparam());
		}
		else if (param instanceof Exprparam)
		{
			Exprparam eParam = (Exprparam) param;
			System.out.print(tabs + "Checking exprParam " + eParam.getName() + inClass);
			
			Class<?> innerClass = fromClass;
			if (eParam.getName() != null)
			{
				Member member = checkMember(eParam, fromClass);
				innerClass = member instanceof Field ? ((Field) member).getType() : ((Method) member).getReturnType();
			}
			System.out.println(": is " + innerClass.getSimpleName());
			
			assertTrue(ParserRuleContext.class.isAssignableFrom(innerClass), "exprParam must be subclass of ParserRuleContext.class but " + fromClass.getSimpleName());
		}
		else if (param instanceof Nonnullparam)
		{
			System.out.println(tabs + "Checking " + param.getClass().getSimpleName() + " " + ((Nonnullparam) param).getName() + inClass);
			checkMember((Nonnullparam) param, fromClass);
		}
	}

	private Member checkMember(Nonnullparam param, Class<?> fromClass)
	{
		boolean hasOrdinal = param.getOrdinal() != null;
		Predicate<Method> checkParams = m -> hasOrdinal ? Arrays.equals(new Class<?>[] { int.class }, m.getParameterTypes()) : m.getParameterCount() == 0;
		
		Optional<? extends Member> member = Optional.empty();
		
		if (param.getName() != null)
			for (String name: param.getName().split("\\|"))
			{
				member = Arrays.stream(fromClass.getFields())
						.filter(f -> f.getName().equals(name))
						.findAny();
				
				if (member.isEmpty())
					member = Arrays.stream(fromClass.getMethods())
							.filter(checkParams)
							.filter(m -> m.getName().equals(name))
							.findAny();
					
				if (member.isEmpty() && Arrays.stream(Vtl.class.getDeclaredClasses()).anyMatch(c -> fromClass == c))
				{
					List<Class<?>> candidates = Arrays.stream(Vtl.class.getDeclaredClasses())
							.filter(fromClass::isAssignableFrom)
							.collect(toList());
					
					for (Class<?> candidate: candidates)
					{
						member = Arrays.stream(candidate.getFields())
								.filter(f -> f.getName().equals(name))
								.findAny();
						if (member.isEmpty())
							member = Arrays.stream(candidate.getMethods())
								.filter(checkParams)
								.filter(m -> m.getName().equals(name))
								.findAny();
						
						if (member.isPresent())
							return member.get();
					}
				}
				
				if (member.isPresent())
					return member.get();
			}
		
		if (param.getName() != null && !member.isPresent())
			assertTrue(param.getName() == null || member.isPresent(), "No field or method called " + param.getName().replaceAll("\\|", " or ") + " for class " + fromClass.getSimpleName());
		
		if (param instanceof Stringparam)
			return null;
		else if (param instanceof Customparam)
			return null;
		else if (param instanceof Valueparam)
			return null;
		else if (param instanceof Aliasparam)
			return null;
		else
			throw new InvalidParameterException(param.getClass().getSimpleName() + " from " + fromClass.getSimpleName());
	}
}
