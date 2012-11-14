package darkfoo.pig.misc;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class SpaceReducer extends EvalFunc<String> {
  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return null;
    try{
      String str = (String)input.get(0);
      String work;
      while(str.indexOf("  ") != -1){
        work = str.replaceAll("  ", " ");
        str = work;
      }
      return str;
    }catch(Exception e){
      return null;
    }
  }
}