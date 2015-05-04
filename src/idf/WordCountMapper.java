package idf;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;

import snowballstemmer.PorterStemmer;

public class WordCountMapper extends Mapper<Text, Text, Text, IntWritable> {

	private static final String DELIMATOR = " \t\n\r\"'-_/.,:;|{}[]!@#%^&*()<>=+`~?";
	private final static IntWritable one = new IntWritable(1);
    
    public static String html2text(String content) {
	    return Jsoup.parse(content).text();
	}
	
    public static String stemContent(String content) {
		StringTokenizer tokenizer = new StringTokenizer(content, DELIMATOR);
		String word = "";
		PorterStemmer stemmer = new PorterStemmer();
		StringBuilder sb = new StringBuilder();
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			if(word.equals("")) continue;
			boolean flag = false;
			for(int i=0;i<word.length();i++){
				if (Character.UnicodeBlock.of(word.charAt(i)) != Character.UnicodeBlock.BASIC_LATIN) {
					flag = true;
					break;
				}
			}	
			if(flag) continue;
			stemmer.setCurrent(word);
			if(stemmer.stem()){
				sb.append(stemmer.getCurrent());
				sb.append(" ");
			}
		}
		return new String(sb);
	}
    
	public void map(Text key, Text value, Context context) {
		// key: docID, value: content
		String content = value.toString().toLowerCase();
		content = html2text(content);

		// stemming the content
		content = stemContent(content);
		
		// process one document
		HashSet<String> wordSet = new HashSet<String>();
		StringTokenizer tokenizer = new StringTokenizer(content, DELIMATOR);
		String word = "";
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			if(word.equals("")) continue;
			if(!wordSet.contains(word)){
				wordSet.add(word);
			}
		}
		for(String w:wordSet){
			try {
				context.write(new Text(w), one);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
