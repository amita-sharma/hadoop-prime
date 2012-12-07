/* 	This file is apart of Hadoop-prime

    Hadoop-prime is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    any later version.

    Hadoop-prime is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Hadoop-prime.  If not, see <http://www.gnu.org/licenses/>.
    
    Copyright 2012 Cameron Cook 
 */
package edu.american.student.util;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.mcavallo.opencloud.Cloud;
import org.mcavallo.opencloud.Tag;

public class CloudGenerator
{

	public static void generateCloud(Cloud cloud, String saveLocation) throws IOException
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
		FileUtils.writeByteArrayToFile(new File(saveLocation), html.getBytes());
	}

	
}
