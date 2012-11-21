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
import java.util.Iterator;
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
c = FOREACH b GENERATE darkfoo.pig.Bags.TokenizeSpace(mess) AS mess;
d = FOREACH c GENERATE darkfoo.pig.Bags.MarkovPair(mess);
DUMP d;
*/

public class MarkovPair extends EvalFunc<DataBag> {

    private static final BagFactory bagFac = BagFactory.getInstance();
    private static final TupleFactory tupleFac = TupleFactory.getInstance();

    @Override
    public DataBag exec(Tuple input) throws IOException {
        try {
            DataBag inbag = (DataBag) input.get(0);
            DataBag res = null;
            if (input != null) {
                res = bagFac.newDefaultBag();
                Iterator<Tuple> it = inbag.iterator();
                Tuple prev = it.next();
                while (it.hasNext()) {
                    Tuple cur = it.next();
                    Tuple tup = tupleFac.newTuple(2);
                    tup.set(0, prev.get(0));
                    tup.set(1, cur.get(0));
                    res.add(tup);
                    prev = cur;
                }
            }
            return res;
        }
        catch (Exception e) {
            throw new RuntimeException("MarkovPair error", e);
        }
    }
}
