import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
//import org.apache.mahout.core.WeightedVectorWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.NamedVector;

public class ClusterOutput {

/**
 * @param args
 */
public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {
                BufferedWriter bw;
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(conf);
                File pointsFolder = new File("opdata/clusters-50");
                File files[] = pointsFolder.listFiles();
                bw = new BufferedWriter(new FileWriter(new File("output")));
                HashMap<String, Integer> clusterIds;
                clusterIds = new HashMap<String, Integer>(5000);
                for(File file:files){
                        if(file.getName().indexOf("part-r-00000")<0)
                                continue;
                        SequenceFile.Reader reader = new SequenceFile.Reader(fs,  new Path(file.getAbsolutePath()), conf);
                        IntWritable key = new IntWritable();
                        WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
                        while (reader.next(key, value)) {
                        	System.out.println(key.toString());
                            System.out.println(value);
                        	if(value==null)
                        		continue;
                                NamedVector vector = (NamedVector) value.getVector();
                                String vectorName = vector.getName();
                                bw.write(vectorName + "\t" + key.toString()+"\n");
                                if(clusterIds.containsKey(key.toString())){
                                        clusterIds.put(key.toString(), clusterIds.get(key.toString())+1);
                                }
                                else
                                        clusterIds.put(key.toString(), 1);
                        }
                        bw.flush();
                        reader.close(); 
                }
                bw.flush();
                bw.close();
                bw = new BufferedWriter(new FileWriter(new File("analysis")));
                Set<String> keys=clusterIds.keySet();
                for(String key:keys){
                        bw.write(key+" "+clusterIds.get(key)+"\n");
                }
                bw.flush();
                bw.close();
                } catch (IOException e) {
                        e.printStackTrace();
                }
        }
}