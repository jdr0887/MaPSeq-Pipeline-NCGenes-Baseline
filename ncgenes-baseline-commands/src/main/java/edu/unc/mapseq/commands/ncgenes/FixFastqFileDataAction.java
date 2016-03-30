package edu.unc.mapseq.commands.ncgenes;

import java.util.List;
import java.util.concurrent.Executors;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.ncgenes.FixFastqFileDataRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;

@Command(scope = "ncgenes", name = "fix-fastq-file-data", description = "Fix Fastq FileData")
@Service
public class FixFastqFileDataAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(FixFastqFileDataAction.class);

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Argument(index = 0, name = "flowcellId", description = "Flowcell Identifier", required = true, multiValued = true)
    private List<Long> flowcellIdList;

    @Override
    public Object execute() throws Exception {
        logger.info("ENTERING execute()");
        FixFastqFileDataRunnable runnable = new FixFastqFileDataRunnable();
        runnable.setMaPSeqDAOBeanService(maPSeqDAOBeanService);
        runnable.setFlowcellIdList(flowcellIdList);
        Executors.newSingleThreadExecutor().execute(runnable);
        return null;
    }

    public List<Long> getFlowcellIdList() {
        return flowcellIdList;
    }

    public void setFlowcellIdList(List<Long> flowcellIdList) {
        this.flowcellIdList = flowcellIdList;
    }

}
