package edu.american.student.util;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.mcavallo.opencloud.Cloud;
import org.mcavallo.opencloud.Tag;

public class CloudGenerator
{

	public static void generateCloud(Cloud cloud, String string) throws IOException
	{
		List<Tag> tags =cloud.allTags();
		String html = "";
		int i=0;
		int tagsPerLine = 30;
		for(Tag tag: tags)
		{
			 int score = tag.getScoreInt();
			 html+="<a href="+tag.getLink()+" style=font-size:"+(score*7)+"px;>"+tag.getName()+"</a>&nbsp;";
			 if(i%tagsPerLine ==0)
			 {
				 html+="<br>";
			 }
			 i++;
		}
		FileUtils.writeByteArrayToFile(new File("/home/cam/Desktop/tagcloud.html"), html.getBytes());
	}

	
}
