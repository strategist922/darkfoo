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
package darkfoo.pig.Cleanup;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/*
Usage:
REGISTER /path/to/jar
a = LOAD '/path/to/data/on/hdfs' USING PigStorage('\t') AS (id: chararray, mess: chararray);
b = FOREACH a GENERATE darkfoo.pig.Cleanup.URLDelete(mess);
DUMP b;
*/

public class URLDelete extends EvalFunc<String>{
  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return null;
    try{
      String str = (String)input.get(0);
      return str.replaceAll("\b(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]", " ");
    }catch(Exception e){
      return null;
    }
  }
}