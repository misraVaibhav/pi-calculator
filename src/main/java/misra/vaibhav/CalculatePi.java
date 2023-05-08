package misra.vaibhav;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static java.lang.Math.pow;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class CalculatePi {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("generate", new IntegerSpout(), 1);
        builder.setBolt("calcterm", new TermBolt(), 1).shuffleGrouping("generate");
        builder.setBolt("calcproduct", new ProductBolt(), 1).shuffleGrouping("calcterm");

        Config conf = new Config();

        conf.setDebug(true);

        String topologyName = "Pi Calculator";

        conf.setNumWorkers(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, builder.createTopology());
    }

    public static class IntegerSpout extends BaseRichSpout {

        private SpoutOutputCollector outputCollector;
        int count;

        @Override
        public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            count = 0;
            outputCollector = spoutOutputCollector;
        }

        @Override
        public void nextTuple() {
            outputCollector.emit(new Values(count++));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("integer"));
        }
    }

    public static class TermBolt extends BaseRichBolt {
        OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        private double pow(double num, int p) {
            double ans = 1;
            while(p > 0) {
                if(p % 2 != 0) {
                    p -= 1;
                    ans *= num;
                } else {
                    num *= num;
                    p /= 2;
                }
            }
            return ans;
        }

        private double fact(int num) {
            double ans = 1;
            while(num > 0) ans *= num--;
            return ans;
        }

        @Override
        public void execute(Tuple tuple) {
            int term = tuple.getIntegerByField("integer");
            double num = fact(4 * term) * (23690 * term + 1103);
            double den = pow(pow(4, term) * fact(term), 4) * pow(99, 4 * term);
            collector.emit(new Values(term, num / den));
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("integer", "term"));
        }

    }


    public static class ProductBolt extends BaseRichBolt {

        private static final Logger log = LoggerFactory.getLogger(ProductBolt.class);
        OutputCollector collector;
        boolean[] present;
        double pi;
        double sum;
        boolean collected;
        int size;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            size = 20;
            sum = 0;
            collected = false;
            present = new boolean[size + 1];
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            double term = tuple.getDoubleByField("term");
            int num = tuple.getIntegerByField("integer");
            boolean val = true;
            for(int i = 0;i <= size; ++i) val &= present[i];
            if(val && !collected) {
                collected = true;
                pi = 1 / ((pow(8, 0.5) / 9801) * (sum));
                log.info("Calculated Pi: ", pi);
            } else if(!collected && num <= size) {
                present[num] = true;
                sum += term;
                log.info("Received term: {} for n = {}", term, num);
            } else if(collected) {
                log.info("Calculated Pi : {}", pi);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }
}