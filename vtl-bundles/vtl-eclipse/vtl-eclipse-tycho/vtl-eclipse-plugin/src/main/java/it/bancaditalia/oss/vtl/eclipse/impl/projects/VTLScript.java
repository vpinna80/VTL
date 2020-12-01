package it.bancaditalia.oss.vtl.eclipse.impl.projects;

import java.net.MalformedURLException;

import org.eclipse.jface.viewers.TreeNode;
import org.eclipse.swt.graphics.Image;

import it.bancaditalia.oss.vtl.eclipse.parts.ImageManager;
import it.bancaditalia.oss.vtl.eclipse.parts.ImageProvider;

public class VTLScript extends TreeNode implements ImageProvider
{
	private static final String NEW_FILE_ICON_URL = "platform:/plugin/org.eclipse.ui/icons/full/obj16/file_obj.png";
	
	public VTLScript(String value)
	{
		super(value);
	}

	@Override
	public Image getImage() throws MalformedURLException
	{
		return ImageManager.getImage(NEW_FILE_ICON_URL);
	}
	
	@Override
	public String toString()
	{
		return (String) getValue();
	}
}
