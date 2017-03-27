
package job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class PageRankJob1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    //creates key value pairs of nodes in graph
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        
        //skip lines with # in dataset
        if (value.charAt(0) != '#') {
            //tab is node separator
            int tabIndex = value.find("\t");
            //get node names
            String nodeA = Text.decode(value.getBytes(), 0, tabIndex);
            String nodeB = Text.decode(value.getBytes(), tabIndex + 1, value.getLength() - (tabIndex + 1));
            context.write(new Text(nodeA), new Text(nodeB));
            
            // add the current source node to the node list so we can 
            // compute the total amount of nodes of our graph in Job#2
            PageRank.NODES.add(nodeA);
            // also add the target node to the same list: we may have a target node 
            // with no outlinks (so it will never be parsed as source)
            PageRank.NODES.add(nodeB);
            
        }
 
    }
    
}
