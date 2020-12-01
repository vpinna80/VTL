package it.bancaditalia.oss.vtl.eclipse.impl.projects;

import java.net.MalformedURLException;

import org.eclipse.jface.viewers.TreeNode;
import org.eclipse.swt.graphics.Image;

import it.bancaditalia.oss.vtl.eclipse.parts.ImageManager;
import it.bancaditalia.oss.vtl.eclipse.parts.ImageProvider;

public class VTLFolder extends TreeNode implements ImageProvider
{
	private static final String FOLDER_ICON_URL = "platform:/plugin/org.eclipse.ui/icons/full/obj16/fldr_obj.png";

	public VTLFolder(String name)
	{
		super(name);
	}
	
	@Override
	public Image getImage() throws MalformedURLException
	{
		return ImageManager.getImage(FOLDER_ICON_URL);
	}
	
	@Override
	public String toString()
	{
		return getValue().toString();
	}
}
