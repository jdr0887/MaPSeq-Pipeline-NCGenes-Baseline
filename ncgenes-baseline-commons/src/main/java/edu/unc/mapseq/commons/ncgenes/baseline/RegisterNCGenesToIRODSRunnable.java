package edu.unc.mapseq.commons.ncgenes.baseline;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.config.MaPSeqConfigurationService;
import edu.unc.mapseq.config.RunModeType;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.workflow.impl.IRODSBean;
import edu.unc.mapseq.workflow.impl.SampleWorkflowUtil;

public class RegisterNCGenesToIRODSRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RegisterNCGenesToIRODSRunnable.class);

    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    private MaPSeqConfigurationService maPSeqConfigurationService;

    private Long flowcellId;

    private Long sampleId;

    @Override
    public void run() {
        logger.info("ENTERING run()");

        RunModeType runMode = getMaPSeqConfigurationService().getRunMode();

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

        for (Sample sample : sampleSet) {
            Flowcell flowcell = sample.getFlowcell();

            File outputDirectory = new File(sample.getOutputDirectory(), "NCGenes");
            File tmpDir = new File(outputDirectory, "tmp");
            if (!tmpDir.exists()) {
                tmpDir.mkdirs();
            }

            List<File> readPairList = SampleWorkflowUtil.getReadPairList(sample);

            // assumption: a dash is used as a delimiter between a participantId and the external code
            int idx = sample.getName().lastIndexOf("-");
            String participantId = idx != -1 ? sample.getName().substring(0, idx) : sample.getName();

            String irodsHome = System.getenv("NCGENES_IRODS_HOME");
            if (StringUtils.isEmpty(irodsHome)) {
                logger.error("irodsHome is not set");
                return;
            }

            String ncgenesIRODSDirectory;

            switch (runMode) {
                case DEV:
                case STAGING:
                    ncgenesIRODSDirectory = String.format("/MedGenZone/home/medgenuser/sequence_data/%s/ncgenes/%s",
                            runMode.toString().toLowerCase(), participantId);
                    break;
                case PROD:
                default:
                    ncgenesIRODSDirectory = String.format("/MedGenZone/home/medgenuser/sequence_data/ncgenes/%s", participantId);
                    break;
            }

            CommandOutput commandOutput = null;

            List<CommandInput> commandInputList = new LinkedList<CommandInput>();

            CommandInput commandInput = new CommandInput();
            commandInput.setExitImmediately(Boolean.FALSE);
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%s/bin/imkdir -p %s%n", irodsHome, ncgenesIRODSDirectory));
            sb.append(String.format("%s/bin/imeta add -C %s Project NCGENES%n", irodsHome, ncgenesIRODSDirectory));
            sb.append(String.format("%s/bin/imeta add -C %s ParticipantID %s NCGENES%n", irodsHome, ncgenesIRODSDirectory, participantId));
            commandInput.setCommand(sb.toString());
            commandInput.setWorkDir(tmpDir);
            commandInputList.add(commandInput);

            List<IRODSBean> files2RegisterToIRODS = new ArrayList<IRODSBean>();

            File r1FastqFile = readPairList.get(0);
            String r1FastqRootName = SampleWorkflowUtil.getRootFastqName(r1FastqFile.getName());
            files2RegisterToIRODS.add(new IRODSBean(r1FastqFile, "fastq", null, null, runMode));

            File r2FastqFile = readPairList.get(1);
            String r2FastqRootName = SampleWorkflowUtil.getRootFastqName(r2FastqFile.getName());
            files2RegisterToIRODS.add(new IRODSBean(r2FastqFile, "fastq", null, null, runMode));

            String fastqLaneRootName = StringUtils.removeEnd(r2FastqRootName, "_R2");

            File writeVCFHeaderOut = new File(outputDirectory, fastqLaneRootName + ".vcf.hdr");
            files2RegisterToIRODS.add(new IRODSBean(writeVCFHeaderOut, "VcfHdr", null, null, runMode));

            File fastqcR1Output = new File(outputDirectory, r1FastqRootName + ".fastqc.zip");
            files2RegisterToIRODS.add(new IRODSBean(fastqcR1Output, "fastqc", null, null, runMode));

            File fastqcR2Output = new File(outputDirectory, r2FastqRootName + ".fastqc.zip");
            files2RegisterToIRODS.add(new IRODSBean(fastqcR2Output, "fastqc", null, null, runMode));

            File bwaSAMPairedEndOutFile = new File(outputDirectory, fastqLaneRootName + ".sam");

            File fixRGOutput = new File(outputDirectory, bwaSAMPairedEndOutFile.getName().replace(".sam", ".fixed-rg.bam"));
            File picardMarkDuplicatesOutput = new File(outputDirectory, fixRGOutput.getName().replace(".bam", ".deduped.bam"));
            File indelRealignerOut = new File(outputDirectory, picardMarkDuplicatesOutput.getName().replace(".bam", ".realign.bam"));
            File picardFixMateOutput = new File(outputDirectory, indelRealignerOut.getName().replace(".bam", ".fixmate.bam"));
            File gatkTableRecalibrationOut = new File(outputDirectory, picardFixMateOutput.getName().replace(".bam", ".recal.bam"));
            files2RegisterToIRODS.add(new IRODSBean(gatkTableRecalibrationOut, "RecalBam", null, null, runMode));

            File gatkTableRecalibrationIndexOut = new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".bai"));
            files2RegisterToIRODS.add(new IRODSBean(gatkTableRecalibrationIndexOut, "RecalBamBai", null, null, runMode));

            files2RegisterToIRODS.add(new IRODSBean(
                    new File(outputDirectory,
                            gatkTableRecalibrationOut.getName().replace(".bam", ".coverage.sample_cumulative_coverage_counts")),
                    "CoverageCounts", null, null, runMode));
            files2RegisterToIRODS
                    .add(new IRODSBean(
                            new File(outputDirectory,
                                    gatkTableRecalibrationOut.getName().replace(".bam",
                                            ".coverage.sample_cumulative_coverage_proportions")),
                            "CoverageProportions", null, null, runMode));
            files2RegisterToIRODS.add(new IRODSBean(
                    new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".coverage.sample_interval_statistics")),
                    "IntervalStatistics", null, null, runMode));
            files2RegisterToIRODS.add(new IRODSBean(
                    new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".coverage.sample_interval_summary")),
                    "IntervalSummary", null, null, runMode));
            files2RegisterToIRODS.add(new IRODSBean(
                    new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".coverage.sample_statistics")),
                    "SampleStatistics", null, null, runMode));
            files2RegisterToIRODS.add(new IRODSBean(
                    new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".coverage.sample_summary")),
                    "SampleSummary", null, null, runMode));

            File samtoolsFlagstatOut = new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".samtools.flagstat"));
            files2RegisterToIRODS.add(new IRODSBean(samtoolsFlagstatOut, "Flagstat", null, null, runMode));

            File gatkFlagstatOut = new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".gatk.flagstat"));
            files2RegisterToIRODS.add(new IRODSBean(gatkFlagstatOut, "GATK.Flagstat", null, null, runMode));

            File filterVariant1Output = new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam", ".variant.vcf"));
            File gatkApplyRecalibrationOut = new File(outputDirectory,
                    filterVariant1Output.getName().replace(".vcf", ".recalibrated.filtered.vcf"));
            files2RegisterToIRODS.add(new IRODSBean(gatkApplyRecalibrationOut, "RecalibratedVcf", null, null, runMode));

            File filterVariant2Output = new File(outputDirectory, filterVariant1Output.getName().replace(".vcf", ".ic_snps.vcf"));
            files2RegisterToIRODS.add(new IRODSBean(filterVariant2Output, "IcSnpsVcf", null, null, runMode));

            for (IRODSBean bean : files2RegisterToIRODS) {

                commandInput = new CommandInput();
                commandInput.setExitImmediately(Boolean.FALSE);

                File f = bean.getFile();
                if (!f.exists()) {
                    logger.warn("file to register doesn't exist: {}", f.getAbsolutePath());
                    continue;
                }

                StringBuilder registerCommandSB = new StringBuilder();
                String registrationCommand = String.format("%s/bin/ireg -f %s %s/%s", irodsHome, bean.getFile().getAbsolutePath(),
                        ncgenesIRODSDirectory, bean.getFile().getName());
                String deRegistrationCommand = String.format("%s/bin/irm -U %s/%s", irodsHome, ncgenesIRODSDirectory,
                        bean.getFile().getName());
                registerCommandSB.append(registrationCommand).append("\n");
                registerCommandSB.append(String.format("if [ $? != 0 ]; then %s; %s; fi%n", deRegistrationCommand, registrationCommand));
                commandInput.setCommand(registerCommandSB.toString());
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

                commandInput = new CommandInput();
                commandInput.setExitImmediately(Boolean.FALSE);
                sb = new StringBuilder();
                sb.append(String.format("%s/bin/imeta add -d %s/%s ParticipantID %s NCGENES%n", irodsHome, ncgenesIRODSDirectory,
                        bean.getFile().getName(), participantId));
                sb.append(String.format("%s/bin/imeta add -d %s/%s FileType %s NCGENES%n", irodsHome, ncgenesIRODSDirectory,
                        bean.getFile().getName(), bean.getType()));
                sb.append(String.format("%s/bin/imeta add -d %s/%s System %s NCGENES%n", irodsHome, ncgenesIRODSDirectory,
                        bean.getFile().getName(), StringUtils.capitalize(bean.getRunMode().toString().toLowerCase())));
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

        }

    }

    public MaPSeqDAOBeanService getMaPSeqDAOBeanService() {
        return maPSeqDAOBeanService;
    }

    public void setMaPSeqDAOBeanService(MaPSeqDAOBeanService maPSeqDAOBeanService) {
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
    }

    public MaPSeqConfigurationService getMaPSeqConfigurationService() {
        return maPSeqConfigurationService;
    }

    public void setMaPSeqConfigurationService(MaPSeqConfigurationService maPSeqConfigurationService) {
        this.maPSeqConfigurationService = maPSeqConfigurationService;
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

}
