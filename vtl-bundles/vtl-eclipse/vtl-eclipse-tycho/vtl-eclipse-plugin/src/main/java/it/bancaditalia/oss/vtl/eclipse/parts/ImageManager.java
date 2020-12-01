package it.bancaditalia.oss.vtl.eclipse.parts;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.swt.graphics.Image;

public class ImageManager
{
	private static final Map<String, Image> IMAGES = new HashMap<>();
	
	private ImageManager()
	{
		
	}
	
	public static Image getImage(String URL) throws MalformedURLException
	{
		if (IMAGES.containsKey(URL))
			return IMAGES.get(URL);
		else
			synchronized (ImageManager.class)
			{
				if (IMAGES.containsKey(URL))
					return IMAGES.get(URL);
				Image image = ImageDescriptor.createFromURL(new URL(URL)).createImage();
				IMAGES.put(URL, image);
				return image;
			}
	}
}
