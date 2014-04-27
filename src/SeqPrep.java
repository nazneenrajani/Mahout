import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

public class SeqPrep {

	public static void main(String args[]) throws IOException{

		List<NamedVector> apples = new ArrayList<NamedVector>();

		NamedVector apple;
		BufferedReader SW;
		List<String[]> vectors = new ArrayList<String[]>();
		try {
			SW= new BufferedReader(new FileReader("dump.csv"));
			for(String lin;(lin = SW.readLine()) != null;){
				String[] tmp = lin.split(",");
				System.out.println(tmp[0]);
				vectors.add(tmp);
			}
			SW.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(vectors.size());
		for(int i =0;i<vectors.size();i++){
			String[] t = vectors.get(i);
			//System.out.println(vectors.get(0)[0]);
			double[] s = {Double.parseDouble(t[3]),Double.parseDouble(t[4]),Double.parseDouble(t[5]),Double.parseDouble(t[6])};
			apple = new NamedVector(new DenseVector(s),String.valueOf(i));        

			apples.add(apple);
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("appledata/apples");

		SequenceFile.Writer writer = new SequenceFile.Writer(fs,  conf, path, Text.class, VectorWritable.class);

		VectorWritable vec = new VectorWritable();
		for(NamedVector vector : apples){
			vec.set(vector);
			writer.append(new Text(vector.getName()), vec);
		}
		writer.close();

		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("appledata/apples"), conf);

		Text key = new Text();
		VectorWritable value = new VectorWritable();
		while(reader.next(key, value)){
			System.out.println(key.toString() + " , " + value.get().asFormatString());
		}
		reader.close();

	}

}