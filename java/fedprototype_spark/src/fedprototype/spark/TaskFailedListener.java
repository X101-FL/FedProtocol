package fedprototype.spark;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.SparkConf;

public class TaskFailedListener extends SparkListener {
    private String coordinater_url = null;

    public TaskFailedListener(SparkConf conf) {
        this.coordinater_url = conf.get("fed.coordinater.url");
        System.out.println("coordinater_url: " + this.coordinater_url);
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