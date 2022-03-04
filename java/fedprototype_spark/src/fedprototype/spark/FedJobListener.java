package fedprototype.spark;

import java.io.IOException;
import java.util.HashMap;

import org.apache.spark.scheduler.SparkListener;

import org.apache.spark.scheduler.SparkListenerTaskEnd;
import fedprototype.tools.Http;

public class FedJobListener extends SparkListener {
    private String coordinaterURL = null;
    private String jobID = null;
    private String rootRoleName = null;

    private boolean enable = true;

    public FedJobListener(
            String coordinaterURL,
            String jobID,
            String rootRoleName) {
        this.coordinaterURL = coordinaterURL;
        this.jobID = jobID;
        this.rootRoleName = rootRoleName;
        this.enable = true;
        System.out.println("new Listener coordinaterURL:" + this.coordinaterURL
                + " jobID:" + this.jobID
                + " rootRoleName:" + this.rootRoleName);
    }

    public void disableListener() {
        this.enable = false;
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        if (!this.enable) {
            return;
        }
        HashMap<String, Object> params = new HashMap<>();
        System.out.println("onTaskEnd:");
        params.put("job_id", this.jobID);
        params.put("root_role_name", this.rootRoleName);
        params.put("partition_id", taskEnd.taskInfo().index());
        params.put("stage_id", taskEnd.stageId());
        params.put("task_attempt_num", taskEnd.taskInfo().attemptNumber());
        params.put("success", taskEnd.taskInfo().successful());
        System.out.println("cancal task partition_id:" + params.get("partition_id")
                + " task_attempt_num:" + taskEnd.taskInfo().attemptNumber()
                + " task state:" + taskEnd.taskInfo().successful());
        // try {
        // Http.post_pro(this.coordinaterURL + "/cancel_task", params);
        // } catch (IOException e) {
        // throw new RuntimeException(e);
        // }
    }

}