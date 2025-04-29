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
package it.bancaditalia.oss.vtl.coverage.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class RepeatedParameterizedTestExtension implements TestTemplateInvocationContextProvider
{
	private static class RPIContext implements TestTemplateInvocationContext, ParameterResolver
	{
		private final String name;
		private final Object[] arguments;

		private RPIContext(int index, int repeats, String pattern, Arguments args)
		{
			arguments = args.get();

			String name = Objects.requireNonNull(pattern)
					.replace("{currentRepetition}", String.valueOf(index))
					.replace("{totalRepetitions}", String.valueOf(repeats));
			for (int i = 0; i < arguments.length; i++)
				name = name.replace("{" + i + "}", String.valueOf(arguments[i] instanceof Path ? ((Path) arguments[i]).getFileName() : arguments[i]));
			
			this.name = name;

			Arrays.stream(arguments).forEach(Objects::requireNonNull);
		}

		@Override
		public String getDisplayName(int invocationIndex)
		{
			return name;
		}

		@Override
		public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException
		{
			return true;
		}

		@Override
		public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException
		{
			return arguments[parameterContext.getIndex()];
		}

		@Override
		public List<Extension> getAdditionalExtensions()
		{
			return List.of(this);
		}
	}

	@Override
	public boolean supportsTestTemplate(ExtensionContext context)
	{
		return context.getTestMethod().isPresent() && context.getTestMethod().get().isAnnotationPresent(RepeatedParameterizedTest.class);
	}
	
	@Override
	public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context)
	{
		Method testMethod = context.getRequiredTestMethod();
		RepeatedParameterizedTest annotation = testMethod.getAnnotation(RepeatedParameterizedTest.class);
		int repeats = annotation.value();
		String namePattern = annotation.name();

		Stream<Arguments> arguments = getArguments(testMethod, context);
		return arguments.flatMap(args -> IntStream.rangeClosed(1, repeats)
				.mapToObj(i -> new RPIContext(i, repeats, namePattern, args)));
	}

	private Stream<Arguments> getArguments(Method testMethod, ExtensionContext context)
	{
		MethodSource methodSource = testMethod.getAnnotation(MethodSource.class);
		if (methodSource == null)
			throw new ParameterResolutionException("No MethodSource annotation found on method: " + testMethod.getName());

		String[] methodNames = methodSource.value();
		String methodName = methodNames.length == 0 ? testMethod.getName() : methodNames[0];

		try
		{
			return ((Stream<?>) context.getRequiredTestClass().getDeclaredMethod(methodName, new Class<?>[0]).invoke(null)).map(Arguments.class::cast);
		}
		catch (NoClassDefFoundError | NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
		{
			throw new ParameterResolutionException("Could not invoke method source: " + methodName, e);
		}
	}
}
