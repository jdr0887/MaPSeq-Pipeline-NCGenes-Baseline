package edu.unc.mapseq.commons.ncgenes.baseline;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.module.sequencing.fastqc.FastQC;
import edu.unc.mapseq.module.sequencing.filter.FilterVariant;
import edu.unc.mapseq.module.sequencing.gatk.GATKApplyRecalibration;
import edu.unc.mapseq.module.sequencing.gatk.GATKDepthOfCoverage;
import edu.unc.mapseq.module.sequencing.gatk.GATKFlagStat;
import edu.unc.mapseq.module.sequencing.gatk.GATKTableRecalibration;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsFlagstat;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsIndex;
import edu.unc.mapseq.workflow.sequencing.IRODSBean;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class RegisterToIRODSRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RegisterToIRODSRunnable.class);

    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    private Long flowcellId;

    private Long sampleId;

    private WorkflowRun workflowRun;

    public RegisterToIRODSRunnable(MaPSeqDAOBeanService maPSeqDAOBeanService, WorkflowRun workflowRun) {
        super();
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
        this.workflowRun = workflowRun;
    }

    @Override
    public void run() {
        logger.info("ENTERING run()");

        Set<Sample> sampleSet = new HashSet<Sample>();

        if (sampleId != null) {
            try {
                sampleSet.add(maPSeqDAOBeanService.getSampleDAO().findById(sampleId));
            } catch (MaPSeqDAOException e1) {
                e1.printStackTrace();
                return;
            }
        }

        if (flowcellId != null) {
            try {
                List<Sample> samples = maPSeqDAOBeanService.getSampleDAO().findByFlowcellId(flowcellId);
                if (samples != null && !samples.isEmpty()) {
                    sampleSet.addAll(samples);
                }
            } catch (MaPSeqDAOException e1) {
                e1.printStackTrace();
                return;
            }
        }

        BundleContext bundleContext = FrameworkUtil.getBundle(getClass()).getBundleContext();
        Bundle bundle = bundleContext.getBundle();
        String version = bundle.getVersion().toString();

        ExecutorService es = Executors.newSingleThreadExecutor();
        for (Sample sample : sampleSet) {
            es.submit(() -> {

                File outputDirectory = SequencingWorkflowUtil.createOutputDirectory(sample, workflowRun.getWorkflow());
                File tmpDir = new File(outputDirectory, "tmp");
                if (!tmpDir.exists()) {
                    tmpDir.mkdirs();
                }

                // assumption: a dash is used as a delimiter between a participantId and the external code
                int idx = sample.getName().lastIndexOf("-");
                String participantId = idx != -1 ? sample.getName().substring(0, idx) : sample.getName();

                String irodsDirectory = String.format("/MedGenZone/%s/sequencing/ncgenes/analysis/%s/%s/%s",
                        workflowRun.getWorkflow().getSystem().getValue(), sample.getFlowcell().getName(), sample.getName(),
                        workflowRun.getWorkflow().getName());

                CommandOutput commandOutput = null;

                List<CommandInput> commandInputList = new LinkedList<CommandInput>();

                CommandInput commandInput = new CommandInput();
                commandInput.setExitImmediately(Boolean.FALSE);
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("$IRODS_HOME/imkdir -p %s%n", irodsDirectory));
                sb.append(String.format("$IRODS_HOME/imeta add -C %s Project NCGENES%n", irodsDirectory));
                sb.append(String.format("$IRODS_HOME/imeta add -C %s ParticipantID %s NCGENES%n", irodsDirectory, participantId));
                commandInput.setCommand(sb.toString());
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

                List<IRODSBean> files2RegisterToIRODS = new ArrayList<IRODSBean>();

                String rootFileName = String.format("%s_%s_L%03d", sample.getFlowcell().getName(), sample.getBarcode(),
                        sample.getLaneIndex());

                List<ImmutablePair<String, String>> attributeList = Arrays.asList(
                        new ImmutablePair<String, String>("ParticipantId", participantId),
                        new ImmutablePair<String, String>("MaPSeqWorkflowVersion", version),
                        new ImmutablePair<String, String>("MaPSeqWorkflowName", getWorkflowRun().getWorkflow().getName()),
                        new ImmutablePair<String, String>("MaPSeqStudyName", sample.getStudy().getName()),
                        new ImmutablePair<String, String>("MaPSeqSampleId", sample.getId().toString()),
                        new ImmutablePair<String, String>("MaPSeqSystem", getWorkflowRun().getWorkflow().getSystem().getValue()),
                        new ImmutablePair<String, String>("MaPSeqFlowcellId", sample.getFlowcell().getId().toString()));

                List<ImmutablePair<String, String>> attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", FastQC.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.APPLICATION_ZIP.toString()));
                files2RegisterToIRODS.add(
                        new IRODSBean(new File(outputDirectory, String.format("%s_R1.fastqc.zip", rootFileName)), attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", FastQC.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.APPLICATION_ZIP.toString()));
                files2RegisterToIRODS.add(
                        new IRODSBean(new File(outputDirectory, String.format("%s_R2.fastqc.zip", rootFileName)), attributeListWithJob));

                File bwaSAMPairedEndOutFile = new File(outputDirectory, String.format("%s.sam", rootFileName));

                File fixRGOutput = new File(outputDirectory, bwaSAMPairedEndOutFile.getName().replace(".sam", ".fixed-rg.bam"));
                File picardMarkDuplicatesOutput = new File(outputDirectory, fixRGOutput.getName().replace(".bam", ".deduped.bam"));
                File indelRealignerOut = new File(outputDirectory, picardMarkDuplicatesOutput.getName().replace(".bam", ".realign.bam"));
                File picardFixMateOutput = new File(outputDirectory, indelRealignerOut.getName().replace(".bam", ".fixmate.bam"));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKTableRecalibration.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.APPLICATION_BAM.toString()));
                File gatkTableRecalibrationOut = new File(outputDirectory, picardFixMateOutput.getName().replace(".bam", ".recal.bam"));
                files2RegisterToIRODS.add(new IRODSBean(gatkTableRecalibrationOut, attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", SAMToolsIndex.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.APPLICATION_BAM_INDEX.toString()));
                files2RegisterToIRODS.add(new IRODSBean(
                        new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".bai")), attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKDepthOfCoverage.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_PLAIN.toString()));
                files2RegisterToIRODS.add(new IRODSBean(
                        new File(outputDirectory,
                                gatkTableRecalibrationOut.getName().replace(".bam", ".coverage.sample_cumulative_coverage_counts")),
                        attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKDepthOfCoverage.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_PLAIN.toString()));
                files2RegisterToIRODS.add(new IRODSBean(
                        new File(outputDirectory,
                                gatkTableRecalibrationOut.getName().replace(".bam", ".coverage.sample_cumulative_coverage_proportions")),
                        attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKDepthOfCoverage.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_PLAIN.toString()));
                files2RegisterToIRODS.add(new IRODSBean(
                        new File(outputDirectory,
                                gatkTableRecalibrationOut.getName().replace(".bam", ".coverage.sample_interval_statistics")),
                        attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKDepthOfCoverage.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_PLAIN.toString()));
                files2RegisterToIRODS.add(new IRODSBean(
                        new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".coverage.sample_interval_summary")),
                        attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKDepthOfCoverage.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_PLAIN.toString()));
                files2RegisterToIRODS.add(new IRODSBean(
                        new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".coverage.sample_statistics")),
                        attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKDepthOfCoverage.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_PLAIN.toString()));
                files2RegisterToIRODS.add(new IRODSBean(
                        new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".coverage.sample_statistics")),
                        attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKDepthOfCoverage.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_PLAIN.toString()));
                files2RegisterToIRODS.add(new IRODSBean(
                        new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".coverage.sample_summary")),
                        attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", SAMToolsFlagstat.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_STAT_SUMMARY.toString()));
                files2RegisterToIRODS.add(
                        new IRODSBean(new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".samtools.flagstat")),
                                attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKFlagStat.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_STAT_SUMMARY.toString()));
                files2RegisterToIRODS
                        .add(new IRODSBean(new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".gatk.flagstat")),
                                attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", FilterVariant.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_VCF.toString()));
                files2RegisterToIRODS
                        .add(new IRODSBean(new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".variant.vcf")),
                                attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKApplyRecalibration.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_VCF.toString()));
                files2RegisterToIRODS.add(new IRODSBean(
                        new File(outputDirectory,
                                gatkTableRecalibrationOut.getName().replace(".bam", ".variant.recalibrated.filtered.vcf")),
                        attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", FilterVariant.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_VCF.toString()));
                files2RegisterToIRODS.add(new IRODSBean(
                        new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".variant.ic_snps.vcf")),
                        attributeListWithJob));

                for (IRODSBean bean : files2RegisterToIRODS) {

                    commandInput = new CommandInput();
                    commandInput.setExitImmediately(Boolean.FALSE);

                    File f = bean.getFile();
                    if (!f.exists()) {
                        logger.warn("file to register doesn't exist: {}", f.getAbsolutePath());
                        continue;
                    }

                    StringBuilder registerCommandSB = new StringBuilder();
                    String registrationCommand = String.format("$IRODS_HOME/ireg -f %s %s/%s", bean.getFile().getAbsolutePath(),
                            irodsDirectory, bean.getFile().getName());
                    String deRegistrationCommand = String.format("$IRODS_HOME/irm -U %s/%s", irodsDirectory, bean.getFile().getName());
                    registerCommandSB.append(registrationCommand).append("\n");
                    registerCommandSB
                            .append(String.format("if [ $? != 0 ]; then %s; %s; fi%n", deRegistrationCommand, registrationCommand));
                    commandInput.setCommand(registerCommandSB.toString());
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);

                    commandInput = new CommandInput();
                    commandInput.setExitImmediately(Boolean.FALSE);
                    sb = new StringBuilder();
                    for (ImmutablePair<String, String> attribute : bean.getAttributes()) {
                        sb.append(String.format("$IRODS_HOME/imeta add -d %s/%s %s %s NCGenes%n", irodsDirectory, bean.getFile().getName(),
                                attribute.getLeft(), attribute.getRight()));
                    }
                    commandInput.setCommand(sb.toString());
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);

                }

                File mapseqrc = new File(System.getProperty("user.home"), ".mapseqrc");
                Executor executor = BashExecutor.getInstance();

                for (CommandInput ci : commandInputList) {
                    try {
                        logger.debug("ci.getCommand(): {}", ci.getCommand());
                        commandOutput = executor.execute(ci, mapseqrc);
                        if (commandOutput.getExitCode() != 0) {
                            logger.info("commandOutput.getExitCode(): {}", commandOutput.getExitCode());
                            logger.warn("command failed: {}", ci.getCommand());
                        }
                        logger.debug("commandOutput.getStdout(): {}", commandOutput.getStdout());
                    } catch (ExecutorException e) {
                        if (commandOutput != null) {
                            logger.warn("commandOutput.getStderr(): {}", commandOutput.getStderr());
                        }
                    }
                }

                logger.info("FINISHED PROCESSING: {}", sample.toString());

            });

        }

    }

    public MaPSeqDAOBeanService getMaPSeqDAOBeanService() {
        return maPSeqDAOBeanService;
    }

    public void setMaPSeqDAOBeanService(MaPSeqDAOBeanService maPSeqDAOBeanService) {
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
    }

    public Long getFlowcellId() {
        return flowcellId;
    }

    public void setFlowcellId(Long flowcellId) {
        this.flowcellId = flowcellId;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

    public WorkflowRun getWorkflowRun() {
        return workflowRun;
    }

    public void setWorkflowRun(WorkflowRun workflowRun) {
        this.workflowRun = workflowRun;
    }

}
