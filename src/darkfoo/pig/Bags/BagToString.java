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
package darkfoo.pig.Bags;

import java.io.IOException;
import java.lang.StringBuilder;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/*
Usage:
REGISTER /path/to/jar
a = LOAD '/path/to/data/on/hdfs' USING PigStorage('\t') AS (id: chararray, text: chararray);
b = FOREACH a GENERATE id AS id, FLATTEN(TokenizeSpace(text)) AS word;
c = GROUP b BY word;
d = FOREACH c {
  id = DISTINCT $1.$0;
  cnt = COUNT(id);
  GENERATE $o as word, cnt as cnt, id as id;
};
--at this point d looks like [word, count, BagOfIds]
e = FOREACH d GENERATE word, cnt, darkfoo.pig.Bags.BagToString(id);
--at this point d looks like [word, count, IdsCommaSeperated]
DUMP e;
*/

public class BagToString extends EvalFunc<String>{
  	
  	public String exec(Tuple tuple) throws IOException{
  		StringBuilder output = new StringBuilder();
    	if (tuple.size() == 0 || tuple.get(0) == null)
      		return "";
    	Object o = tuple.get(0);
    	if (o instanceof DataBag){
      		DataBag input = (DataBag)o;
      		for(Tuple t : input){
      			output.append((String)t.get(0));
      			output.append(",");
      		}
      		output.deleteCharAt(output.length()-1);
      		return output.toString();
    	}else
      		throw new IllegalArgumentException("expected a null or a bag");
  	}

}