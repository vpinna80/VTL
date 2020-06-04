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
package it.bancaditalia.oss.vtl.util;

import static java.awt.EventQueue.invokeLater;
import static java.util.function.UnaryOperator.identity;

import java.awt.BorderLayout;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JProgressBar;
import javax.swing.Timer;

public class ProgressWindow
{
	private final JFrame window;
	
	private AtomicInteger progress = new AtomicInteger();

	private static <T, K> Stream<T> streamHelper(String title, int maxValue, K source, Function<? super K, ? extends Stream<T>> streamProducer)
	{
		ProgressWindow window = new ProgressWindow(title, maxValue);
		return Stream.of(source)
				.map(streamProducer.andThen(s -> s.onClose(window::dispose).peek(i -> window.progress())))
				.reduce(Stream::concat)
				.orElse(Stream.empty());
	}

	private static <K> IntStream intStreamHelper(String title, int maxValue, K source, Function<? super K, ? extends IntStream> streamProducer)
	{
		ProgressWindow window = new ProgressWindow(title, maxValue);
		try (IntStream watchedStream = streamProducer.apply(source).onClose(window::dispose))
		{
			return watchedStream.peek(i -> window.progress());
		}
	}

	public static <T> Stream<T> of(String title, Collection<T> source)
	{
		return streamHelper(title, source.size(), source, Utils::getStream);
	}

	public static <T> Stream<T> of(String title, int maxValue, Stream<T> source)
	{
		
		return streamHelper(title, maxValue, source, identity());
	}

	public static IntStream of(String title, int maxValue)
	{
		return intStreamHelper(title, maxValue, maxValue, Utils::getStream);
	}

	private ProgressWindow(String title, int maxValue)
	{
		if (maxValue > 1000)
		{
			window = new JFrame();
			JProgressBar progressBar = new JProgressBar();
			Timer timer = new Timer(200, event -> invokeLater(() -> progressBar.setValue(progress.get())));
	
			window.setTitle(title);
			window.setSize(400, 100);
			window.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
			window.addWindowListener(new WindowAdapter() {
				
					@Override
					public void windowOpened(WindowEvent e) 
					{
						timer.start();
					};

					@Override
					public void windowClosing(WindowEvent e)
					{
						timer.stop();
					}
				});
			window.setLocationRelativeTo(null);
			window.getContentPane().add(new JLabel("Progress:"), BorderLayout.NORTH);
			
			progressBar.setMaximum(maxValue);
			progressBar.setMinimum(0);
			window.getContentPane().add(progressBar, BorderLayout.CENTER);
			window.setVisible(true);
			
			timer.start();
		}
		else
			window = null;
	}
	
	public void progress()
	{
		progress.incrementAndGet();
	}
	
	public void dispose()
	{
		if (window != null)
			invokeLater(() -> window.dispose());
	}
}
