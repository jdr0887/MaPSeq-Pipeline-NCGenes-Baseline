package edu.unc.mapseq.commons.ncgenes.baseline;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class SaveMarkDuplicatesAttributesRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SaveMarkDuplicatesAttributesRunnable.class);

    private Long sampleId;

    private Long flowcellId;

    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    public SaveMarkDuplicatesAttributesRunnable(MaPSeqDAOBeanService maPSeqDAOBeanService) {
        super();
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
    }

    @Override
    public void run() {
        logger.info("ENTERING run()");

        Set<Sample> sampleSet = new HashSet<Sample>();

        List<Workflow> workflowList = null;
        try {
            if (flowcellId != null) {
                sampleSet.addAll(maPSeqDAOBeanService.getSampleDAO().findByFlowcellId(flowcellId));
            }

            if (sampleId != null) {
                Sample sample = maPSeqDAOBeanService.getSampleDAO().findById(sampleId);
                if (sample == null) {
                    logger.error("Sample was not found");
                    return;
                }
                sampleSet.add(sample);
            }
            workflowList = maPSeqDAOBeanService.getWorkflowDAO().findByName("NCGenesBaseline");
            if (CollectionUtils.isEmpty(workflowList)) {
                return;
            }
        } catch (MaPSeqDAOException e) {
            logger.warn("MaPSeqDAOException", e);
        }

        Workflow workflow = workflowList.get(0);

        for (Sample sample : sampleSet) {

            File outputDirectory = SequencingWorkflowUtil.createOutputDirectory(sample, workflow);

            Set<Attribute> attributeSet = sample.getAttributes();

            Set<String> attributeNameSet = new HashSet<String>();

            for (Attribute attribute : attributeSet) {
                attributeNameSet.add(attribute.getName());
            }

            Set<String> synchSet = Collections.synchronizedSet(attributeNameSet);

            Collection<File> fileList = FileUtils.listFiles(outputDirectory, FileFilterUtils.suffixFileFilter(".deduped.metrics"), null);

            if (CollectionUtils.isNotEmpty(fileList)) {
                File picardMarkDuplicatesMetricsFile = fileList.iterator().next();
                try {
                    List<String> lines = FileUtils.readLines(picardMarkDuplicatesMetricsFile);
                    if (lines != null) {
                        Iterator<String> lineIter = lines.iterator();
                        while (lineIter.hasNext()) {
                            String line = lineIter.next();
                            if (line.startsWith("LIBRARY")) {
                                String nextLine = lineIter.next();
                                String[] split = StringUtils.split(nextLine);

                                String readPairDuplicates = split[6];
                                if (synchSet.contains("PicardMarkDuplicates.readPairDuplicates")) {
                                    for (Attribute attribute : attributeSet) {
                                        if (attribute.getName().equals("PicardMarkDuplicates.readPairDuplicates")) {
                                            attribute.setValue(readPairDuplicates);
                                            break;
                                        }
                                    }
                                } else {
                                    attributeSet.add(new Attribute("PicardMarkDuplicates.readPairDuplicates", readPairDuplicates));
                                }

                                String percentDuplication = split[7];
                                if (synchSet.contains("PicardMarkDuplicates.percentDuplication")) {
                                    for (Attribute attribute : attributeSet) {
                                        if (attribute.getName().equals("PicardMarkDuplicates.percentDuplication")) {
                                            attribute.setValue(percentDuplication);
                                            break;
                                        }
                                    }
                                } else {
                                    attributeSet.add(new Attribute("PicardMarkDuplicates.percentDuplication", percentDuplication));
                                }

                                break;
                            }

                        }
                    }
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

                try {
                    sample.setAttributes(attributeSet);
                    maPSeqDAOBeanService.getSampleDAO().save(sample);
                } catch (MaPSeqDAOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

    public Long getFlowcellId() {
        return flowcellId;
    }

    public void setFlowcellId(Long flowcellId) {
        this.flowcellId = flowcellId;
    }

    public MaPSeqDAOBeanService getMaPSeqDAOBeanService() {
        return maPSeqDAOBeanService;
    }

    public void setMaPSeqDAOBeanService(MaPSeqDAOBeanService maPSeqDAOBeanService) {
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
    }

}
