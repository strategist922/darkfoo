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

import java.io.*;
import java.util.*;

import java.io.BufferedReader;
import java.io.FileReader;

public class WordWebGenerate{
	private static Map<String, String> data;
	public static void main(String[] args) {
        data = new TreeMap<String, String>();
        if(args.length < 2)
            System.out.println("Have to specify source dict and type of words in dict eg. java WordWebGenerate data.noun noun");
        else{
            loadDict(args[0]);
            buildDicts(args[1]);
        }
	}

    private static void buildDicts(String dict){
        try{
            char last = 'a';
            PrintWriter out = new PrintWriter(new File("dict/" + last + '.' + dict));
            for (Map.Entry<String,String> entry : data.entrySet()) {
                if(entry.getKey().charAt(0) != last){
                    out.close();
                    last = entry.getKey().charAt(0);
                    out = new PrintWriter(new File("dict/" + last + '.' + dict));
                }
                out.println(entry.getKey() + "\t" + entry.getValue());
            }
            out.close();
        }catch(IOException e){
            //something
        }
    }

	private static void loadDict(String dir){
		InputStream inStream = null;
        BufferedReader reader = null;
        try{
            inStream = new FileInputStream(dir);
            reader = new BufferedReader(new InputStreamReader(inStream));

            String line = null;
            while((line = reader.readLine()) != null){
                process(line);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                }
            }

            if (inStream != null) {
                try {
                    inStream.close();
                } catch (IOException e) {
                }
            }
        }
	}
	private static void process(String line){
		if((line.length() > 17) && (line.charAt(0) == '0')){
            line = line.substring(17);
            StringTokenizer st = new StringTokenizer(line, " ");
            Set<String> work = new HashSet<String>();
            while(st.hasMoreElements()){
                String word = st.nextToken();

                if(word.startsWith("00")){
                    break;
                }

                if(word.length() > 2 && Character.isLetter(word.charAt(0)) && word.indexOf('_') == -1 && word.indexOf('-') == -1 && word.indexOf('(') == -1 && word.indexOf(')') == -1 && word.indexOf('\'') == -1 && word.indexOf('.') == -1 ){
                    work.add(word.toLowerCase());
                }
            }
            if (work.size() >= 1) {
                StringBuilder syns = new StringBuilder();
                work = Collections.unmodifiableSet(work);
                for (String word : work) {
                    syns.append(word);
                    syns.append(",");
                }
                syns.deleteCharAt(syns.length()-1);
                for(String word : work)
                    data.put(word, syns.toString());
            }
        }
	}
}