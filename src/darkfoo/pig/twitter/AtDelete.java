package darkfoo.pig.twitter;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class AtDelete extends EvalFunc<String>{
  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return null;
    try{
      String str = (String)input.get(0);
      return str.replaceAll("(\\s|\\A)@(\\w+)", " ");
    }catch(Exception e){
      return null;
    }
  }
}