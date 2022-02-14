package fedprototype.spark;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

public class TaskFailedListener extends SparkListener {
    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        System.out.println("################# onOtherEvent ###################");
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        System.out.println("################# onTaskEnd ###################");
        System.out.println("stageId() : " + taskEnd.stageId());
        System.out.println("stageAttemptId() : " + taskEnd.stageAttemptId());
        System.out.println("reason() : " + taskEnd.reason());
        System.out.println("taskInfo() : " + taskEnd.taskInfo());
        System.out.println("taskEnd.taskInfo().successful() : " + taskEnd.taskInfo().successful());
        System.out.println("taskEnd.taskInfo().index() : " + taskEnd.taskInfo().index());
        System.out.println("taskEnd.taskInfo().successful() : " + taskEnd.taskInfo().successful());
    }
}