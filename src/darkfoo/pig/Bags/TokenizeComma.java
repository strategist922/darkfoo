/*
Copyright (c) 2012 Johan Gustavsson <johan@life-hack.org>

This is derivative work of the TOKENIZER class in the Apache Pig trunk.
If there are any complaints, claims or comments please contact me so we 
can solve it in a civilized manner.

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
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.FuncSpec;

/*
Usage:
REGISTER /path/to/jar
a = LOAD '/path/to/data/on/hdfs' USING PigStorage('\t') AS (id: chararray, mess: chararray);
b = FILTER a by mess IS NOT null;
c = FOREACH b GENERATE darkfoo.pig.Bags.TokenizeComma(mess);
DUMP c;
*/

public class TokenizeComma extends EvalFunc<DataBag> {
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();

    @Override
    public DataBag exec(Tuple input) throws IOException {
        try {
            if (input==null)
                return null;
            if (input.size()==0)
                return null;
            Object o = input.get(0);
            if (o==null)
                return null;
            DataBag output = mBagFactory.newDefaultBag();
            if (!(o instanceof String)) {
                int errCode = 2114;
                String msg = "Expected input to be chararray, but" +
                " got " + o.getClass().getName();
                throw new ExecException(msg, errCode, PigException.BUG);
            }
            StringTokenizer tok = new StringTokenizer((String)o, ",", false);
            while (tok.hasMoreTokens()) {
                output.add(mTupleFactory.newTuple(tok.nextToken()));
            }
            return output;
        } catch (ExecException ee) {
            throw ee;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        
        try {
            Schema.FieldSchema tokenFs = new Schema.FieldSchema("token", 
                    DataType.CHARARRAY); 
            Schema tupleSchema = new Schema(tokenFs);

            Schema.FieldSchema tupleFs;
            tupleFs = new Schema.FieldSchema("tuple_of_tokens", tupleSchema,
                    DataType.TUPLE);

            Schema bagSchema = new Schema(tupleFs);
            bagSchema.setTwoLevelAccessRequired(true);
            Schema.FieldSchema bagFs = new Schema.FieldSchema(
                        "bag_of_tokenTuples",bagSchema, DataType.BAG);
            
            return new Schema(bagFs); 
            
            
            
        } catch (FrontendException e) {
            // throwing RTE because
            //above schema creation is not expected to throw an exception
            // and also because superclass does not throw exception
            throw new RuntimeException("Unable to compute TOKENIZE schema.");
        }   
    }

    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    }
};