package it.bancaditalia.oss.vtl.eclipse.parts;

import java.net.MalformedURLException;

import org.eclipse.swt.graphics.Image;

public interface ImageProvider
{
	Image getImage() throws MalformedURLException;
}
