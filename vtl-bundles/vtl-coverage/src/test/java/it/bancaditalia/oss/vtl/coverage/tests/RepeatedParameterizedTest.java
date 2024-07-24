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
package it.bancaditalia.oss.vtl.coverage.tests;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(RepeatedParameterizedTestExtension.class)
@ParameterizedTest
public @interface RepeatedParameterizedTest
{
	/**
	 * The number of repetitions.
	 *
	 * @return the number of repetitions; must be greater than zero
	 */
	int value();
	
	/**
	 * The display name to be used for individual invocations of the
	 * parameterized test; never blank or consisting solely of whitespace.
	 *
	 * <p>Defaults to <code>{default_display_name}</code>.
	 *
	 * <p>If the default display name flag (<code>{default_display_name}</code>)
	 * is not overridden, JUnit will:
	 * <ul>
	 * <li>Look up the {@value ParameterizedTestExtension#DISPLAY_NAME_PATTERN_KEY}
	 * <em>configuration parameter</em> and use it if available. The configuration
	 * parameter can be supplied via the {@code Launcher} API, build tools (e.g.,
	 * Gradle and Maven), a JVM system property, or the JUnit Platform configuration
	 * file (i.e., a file named {@code junit-platform.properties} in the root of
	 * the class path). Consult the User Guide for further information.</li>
	 * <li>Otherwise, the value of the {@link #DEFAULT_DISPLAY_NAME} constant will
	 * be used.</li>
	 * </ul>
	 *
	 * <h4>Supported placeholders</h4>
	 * <ul>
	 * <li>{@link #DISPLAY_NAME_PLACEHOLDER}</li>
	 * <li>{@link #INDEX_PLACEHOLDER}</li>
	 * <li>{@link #ARGUMENTS_PLACEHOLDER}</li>
	 * <li>{@link #ARGUMENTS_WITH_NAMES_PLACEHOLDER}</li>
	 * <li><code>{0}</code>, <code>{1}</code>, etc.: an individual argument (0-based)</li>
	 * </ul>
	 *
	 * <p>For the latter, you may use {@link java.text.MessageFormat} patterns
	 * to customize formatting. Please note that the original arguments are
	 * passed when formatting, regardless of any implicit or explicit argument
	 * conversions.
	 *
	 * <p>Note that <code>{default_display_name}</code> is a flag rather than a
	 * placeholder.
	 *
	 * @see java.text.MessageFormat
	 */
	String name();
}