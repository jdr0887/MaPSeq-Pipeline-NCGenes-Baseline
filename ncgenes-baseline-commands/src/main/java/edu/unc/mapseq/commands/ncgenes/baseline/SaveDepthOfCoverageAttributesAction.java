package edu.unc.mapseq.commands.ncgenes.baseline;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.ncgenes.baseline.SaveDepthOfCoverageAttributesRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;

@Command(scope = "ncgenes-baseline", name = "save-depth-of-coverage-attributes", description = "Save DOC Attributes")
@Service
public class SaveDepthOfCoverageAttributesAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(SaveDepthOfCoverageAttributesAction.class);

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Option(name = "--workflowRunAttemptId", description = "WorkflowRunAttempt Identifier", required = true, multiValued = false)
    private Long workflowRunAttemptId;

    @Override
    public Object execute() throws Exception {
        logger.debug("ENTERING execute()");

        ExecutorService es = Executors.newSingleThreadExecutor();
        WorkflowRunAttempt attempt = maPSeqDAOBeanService.getWorkflowRunAttemptDAO().findById(workflowRunAttemptId);
        SaveDepthOfCoverageAttributesRunnable runnable = new SaveDepthOfCoverageAttributesRunnable(maPSeqDAOBeanService, attempt);
        es.submit(runnable);
        es.shutdown();

        return null;
    }

    public Long getWorkflowRunAttemptId() {
        return workflowRunAttemptId;
    }

    public void setWorkflowRunAttemptId(Long workflowRunAttemptId) {
        this.workflowRunAttemptId = workflowRunAttemptId;
    }

}
