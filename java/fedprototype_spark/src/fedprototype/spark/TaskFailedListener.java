package fedprototype.spark;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerSpeculativeTaskSubmitted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskStart;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class TaskFailedListener extends SparkListener {
    public TaskFailedListener(SparkConf conf) {
        for (Tuple2<String, String> kv : conf.getAll()) {
            System.out.println("key : " + kv._1() + " value : " + kv._2());
        }
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        System.out.println("################# onTaskEnd ###################");
        System.out.println("stageId() : " + taskEnd.stageId());
        System.out.println("reason() : " + taskEnd.reason());
        System.out.println("taskEnd.taskInfo().successful() : " + taskEnd.taskInfo().successful());
        System.out.println("taskEnd.taskInfo().attemptNumber() : " + taskEnd.taskInfo().attemptNumber());
        System.out.println("taskEnd.taskInfo().index() : " + taskEnd.taskInfo().index());
        System.out.println("taskEnd.taskInfo().id() : " + taskEnd.taskInfo().id());
        System.out.println("taskEnd.taskInfo().taskId() : " + taskEnd.taskInfo().taskId());

    }

}