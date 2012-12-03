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

import java.util.List;
import java.util.Arrays;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/*
Usage:
REGISTER /path/to/jar
a = LOAD '/path/to/data/on/hdfs' USING PigStorage('\t') AS (id: chararray, mess: chararray);
b = FOREACH a GENERATE darkfoo.pig.Cleanup.DelHtml(mess);
DUMP b;
*/

public class DelHtml extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		List<String> TagsToDel = Arrays.asList("&quot;","&amp;","&lt;","&gt;","&nbsp;","&iexcl;","&cent;",
								  "&pound;","&curren;","&yen;","&brvbar;","&sect;","&uml;",
								  "&copy;","&ordf;","&laquo;","&not;","&shy;","&reg;",
								  "&macr;","&deg;","&plusmn;","&sup2;","&sup3;","&acute;",
								  "&micro;","&para;","&middot;","&cedil;","&sup1;",
								  "&ordm;","&raquo;","&frac14;","&frac12;","&frac34;",
								  "&iquest;","&Agrave;","&Aacute;","&Acirc;","&Atilde;",
								  "&Auml;","&Aring;","&AElig;","&Ccedil;","&Egrave;",
								  "&Eacute;","&Ecirc;","&Euml;","&Igrave;","&Iacute;",
								  "&Icirc;","&Iuml;","&ETH;","&Ntilde;","&Ograve;",
								  "&Oacute;","&Ocirc;","&Otilde;","&Ouml;","&times;",
								  "&Oslash;","&Ugrave;","&Uacute;","&Ucirc;","&Uuml;",
								  "&Yacute;","&THORN;","&szlig;","&agrave;","&aacute;",
								  "&acirc;","&atilde;","&auml;","&aring;","&aelig;",
								  "&ccedil;","&egrave;","&eacute;","&ecirc;","&euml;",
								  "&igrave;","&iacute;","&icirc;","&iuml;","&eth;",
								  "&ntilde;","&ograve;","&oacute;","&ocirc;","&otilde;",
								  "&ouml;","&divide;","&oslash;","&ugrave;","&uacute;",
								  "&ucirc;","&uuml;","&yacute;","&thorn;","&yuml;",
								  "&OElig;","&oelig;","&Scaron;","&scaron;","&Yuml;",
								  "&fnof;","&circ;","&tilde;","&Alpha;","&Beta;",
								  "&Gamma;","&Delta;","&Epsilon;","&Zeta;","&Eta;",
								  "&Theta;","&Iota;","&Kappa;","&Lambda;","&Mu;","&Nu;",
								  "&Xi;","&Omicron;","&Pi;","&Rho;","&Sigma;","&Tau;",
								  "&Upsilon;","&Phi;","&Chi;","&Psi;","&Omega;",
								  "&alpha;","&beta;","&gamma;","&delta;","&epsilon;",
								  "&zeta;","&eta;","&theta;","&iota;","&kappa;",
								  "&lambda;","&mu;","&nu;","&xi;","&omicron;","&pi;",
								  "&rho;","&sigmaf;","&sigma;","&tau;","&upsilon;",
								  "&phi;","&chi;","&psi;","&omega;","&thetasym;",
								  "&upsih;","&piv;","&ensp;","&emsp;","&thinsp;",
								  "&zwnj;","&zwj;","&lrm;","&rlm;","&ndash;","&mdash;",
								  "&lsquo;","&rsquo;","&sbquo;","&ldquo;","&rdquo;",
								  "&bdquo;","&dagger;","&Dagger;","&bull;","&hellip;",
								  "&permil;","&prime;","&Prime;","&lsaquo;","&rsaquo;",
								  "&oline;","&frasl;","&euro;","&image;","&weierp;",
								  "&real;","&trade;","&alefsym;","&larr;","&uarr;",
								  "&rarr;","&darr;","&harr;","&crarr;","&lArr;","&uArr;",
								  "&rArr;","&dArr;","&hArr;","&forall;","&part;",
								  "&exist;","&empty;","&nabla;","&isin;","&notin;",
								  "&ni;","&prod;","&sum;","&minus;","&lowast;","&radic;",
								  "&prop;","&infin;","&ang;","&and;","&or;","&cap;",
								  "&cup;","&int;","&there4;","&sim;","&cong;","&asymp;",
								  "&ne;","&equiv;","&le;","&ge;","&sub;","&sup;",
								  "&nsub;","&sube;","&supe;","&oplus;","&otimes;",
								  "&perp;","&sdot;","&lceil;","&rceil;","&lfloor;",
								  "&rfloor;","&lang;","&rang;","&loz;","&spades;",
								  "&clubs;","&hearts;","&diams;");
		if (input == null || input.size() == 0)
			return null;
		try{
			String str = (String)input.get(0);
			String work;
			for(String tag : TagsToDel){
				if(str.indexOf(tag) != -1){
					work = str.replaceAll(tag, " ");
					str = work;
				}
			}
			return str;
		}catch(Exception e){
			return null;
		}
	}
}