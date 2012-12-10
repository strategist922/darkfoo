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

package darkfoo.pig.WordNet;

import java.io.*;
import java.util.*;
import java.lang.*;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;

/*
Usage:
REGISTER /path/to/jar
a = LOAD '/path/to/data/on/hdfs' USING PigStorage('\t') AS (id: chararray, mess: chararray);
b = FILTER a by mess IS NOT null;
c = FOREACH b GENERATE FLATTEN(darkfoo.pig.Bags.TokenizeSpace(mess)) AS word;
d = FOREACH c GENERATE darkfoo.pig.WordNet.Search(word);
DUMP d;
*/

public class Search extends EvalFunc<Tuple> {

    private static final TupleFactory tupleFac = TupleFactory.getInstance();

    public Tuple exec(Tuple input) throws IOException {
        String instring = (String) input.get(0);
        if(instring != null){
            try {
                String adj = null;
                String adv = null;
                String verb = null;
                String noun = null;
                InputStream inStream = Search.class.getResourceAsStream("/darkfoo/dict" + instring.charAt(0) + ".adj");
                BufferedReader readbuffer = new BufferedReader(new InputStreamReader(inStream));
                String strRead;
                while ((strRead=readbuffer.readLine())!=null){
                    String splitarray[] = strRead.split("\t");
                    if(instring.equals(splitarray[0])){
                        adj = splitarray[1];
                    }
                }
                inStream = Search.class.getResourceAsStream("/darkfoo/dict" + instring.charAt(0) + ".adv");
                readbuffer = new BufferedReader(new InputStreamReader(inStream));
                while ((strRead=readbuffer.readLine())!=null){
                    String splitarray[] = strRead.split("\t");
                    if(instring.equals(splitarray[0])){
                        adv = splitarray[1];
                    }
                }
                inStream = Search.class.getResourceAsStream("/darkfoo/dict" + instring.charAt(0) + ".verb");
                readbuffer = new BufferedReader(new InputStreamReader(inStream));
                while ((strRead=readbuffer.readLine())!=null){
                    String splitarray[] = strRead.split("\t");
                    if(instring.equals(splitarray[0])){
                        verb = splitarray[1];
                    }
                }
                inStream = Search.class.getResourceAsStream("/darkfoo/dict" + instring.charAt(0) + ".noun");
                readbuffer = new BufferedReader(new InputStreamReader(inStream));
                while ((strRead=readbuffer.readLine())!=null){
                    String splitarray[] = strRead.split("\t");
                    if(instring.equals(splitarray[0])){
                        noun = splitarray[1];
                    }
                }
                Tuple res = tupleFac.newTuple(5);
                res.set(0, instring);
                res.set(1, adj);
                res.set(2, adv);
                res.set(3, verb);
                res.set(4, noun);
            
                return res;
            }catch(Exception e){
                throw new RuntimeException("WordWeb Search error", e);
            }
        }
        return null;
    }
}