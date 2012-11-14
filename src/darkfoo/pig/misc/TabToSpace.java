package darkfoo.pig.misc;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class TabToSpace extends EvalFunc<String>{
  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return null;
    try{
      String str = (String)input.get(0);
      return str.replaceAll("\t", " ");
    }catch(Exception e){
      return null;
    }
  }
}