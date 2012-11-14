package darkfoo.pig.misc;
import java.io.IOException;
import java.util.Iterator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;


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
