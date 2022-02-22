package fedprototype.spark;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import fedprototype.tools.Http;

public class FedJobListener extends SparkListener {
    private String coordinaterURL = null;
    private String jobID = null;
    private String rootRoleName = null;
    private boolean enable = true;
    private boolean success = false;
    private int partition_num = 0;
    private List<String> rootRoleNameSet = null;

    public FedJobListener(
            String coordinaterURL,
            String jobID,
            String rootRoleName,
            List<String> rootRoleNameSet,
            int partition_num) {
        this.coordinaterURL = coordinaterURL;
        this.jobID = jobID;
        this.rootRoleName = rootRoleName;
        this.rootRoleNameSet = rootRoleNameSet;
        this.partition_num = partition_num;
        this.enable = false;
        System.out.println("new Listener coordinaterURL:" + this.coordinaterURL
                + " jobID:" + this.jobID
                + " rootRoleName:" + this.rootRoleName);
    }

    public void markJobState(boolean success) {
        if (!this.enable) {
            return;
        }
        this.success = success;
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        HashMap<String, Object> params = new HashMap<>();
        params.put("job_id", this.jobID);
        params.put("root_role_name", this.rootRoleName);
        params.put("root_role_name_set", this.rootRoleNameSet);
        params.put("partition_num", this.partition_num);
        try {
            Http.post_pro(this.coordinaterURL + "/register_driver", params);
            System.out.println("register driver successfully");
            this.enable = true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        if (!this.enable) {
            return;
        }
        HashMap<String, Object> params = new HashMap<>();
        params.put("job_id", this.jobID);
        params.put("root_role_name", this.rootRoleName);
        params.put("success", this.success);
        try {
            Http.post_pro(this.coordinaterURL + "/cancel_driver", params);
            System.out.println("cancel driver, job state:" + this.success);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.enable = false;
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        if (!this.enable) {
            return;
        }
        HashMap<String, Object> params = new HashMap<>();
        params.put("job_id", this.jobID);
        params.put("root_role_name", this.rootRoleName);
        params.put("partition_id", taskEnd.taskInfo().index());
        params.put("stage_id", taskEnd.stageId());
        params.put("task_attempt_num", taskEnd.taskInfo().attemptNumber());
        params.put("success", taskEnd.taskInfo().successful());
        try {
            Http.post_pro(this.coordinaterURL + "/cancel_task", params);
            System.out.println("cancal task partition_id:" + params.get("partition_id")
                    + " task state:" + taskEnd.taskInfo().successful());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}