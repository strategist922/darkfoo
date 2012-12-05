/*
Copyright (c) 2012 Johan Gustavsson <johan@life-hack.org>

Darkfoo is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Darkfoo is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Darkfoo.  If not, see <http://www.gnu.org/licenses/>.
*/
package darkfoo.pig.Twitter;

import java.util.List;
import java.util.Arrays;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/*
Usage:
REGISTER /path/to/jar
a = LOAD '/path/to/data/on/hdfs' USING PigStorage('\t') AS (id: chararray, mess: chararray);
b = FOREACH a GENERATE darkfoo.pig.Twitter.RTDelete(mess);
DUMP b;
*/

public class RTDelete extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0)
			return null;
		try{
			String str = (String)input.get(0);
			String search = str.toLowerCase();
			if(search.indexOf("rt") != -1){
				if(search.charAt(0) == 'r' && search.charAt(1) == 't'){
					if(search.charAt(2) == ' ' || search.charAt(2) == ':'){
						str = str.substring(3);
					}else if(search.charAt(2) == '@'){
						str = str.substring(2);
					}
				}else if(search.indexOf(" rt ") != -1){
					str = str.replaceAll(" rt ", " ");
					str = str.replaceAll(" Rt ", " ");
					str = str.replaceAll(" rt ", " ");
					str = str.replaceAll(" RT ", " ");
				}else{
					for(int i = 0, n = str.length(); i < n; i++){
						if(search.charAt(i) == 'r' && search.charAt(i+1) == 't' 
						   && !Character.isLetter(search.charAt(i-1)) 
						   && !Character.isLetter(search.charAt(i+2))){
							String work = str.substring(0,i) + " " + str.substring(i+2);
							str = work;
						}
					}
				}
			}
			return str;
		}catch(Exception e){
			return null;
		}

	}
}