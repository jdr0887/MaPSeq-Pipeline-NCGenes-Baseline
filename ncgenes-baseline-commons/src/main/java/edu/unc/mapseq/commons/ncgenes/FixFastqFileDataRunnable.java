package edu.unc.mapseq.commons.ncgenes;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;

public class FixFastqFileDataRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(FixFastqFileDataRunnable.class);

    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    private List<Long> flowcellIdList;

    public FixFastqFileDataRunnable() {
        super();
    }

    @Override
    public void run() {
        logger.debug("ENTERING run()");

        List<Flowcell> flowcellList = new ArrayList<Flowcell>();

        for (Long flowcellId : flowcellIdList) {
            try {
                Flowcell flowcell = maPSeqDAOBeanService.getFlowcellDAO().findById(flowcellId);
                if (flowcell != null) {
                    flowcellList.add(flowcell);
                }
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }
        }

        String outputDir = System.getenv("MAPSEQ_OUTPUT_DIRECTORY");

        if (!flowcellList.isEmpty()) {

            for (Flowcell flowcell : flowcellList) {

                List<Sample> sampleList = null;
                try {
                    sampleList = maPSeqDAOBeanService.getSampleDAO().findByFlowcellId(flowcell.getId());
                } catch (MaPSeqDAOException e) {
                    e.printStackTrace();
                }

                if (sampleList == null) {
                    logger.warn("sampleList was null");
                    continue;
                }

                String path = String.format("%s/%s/CASAVA", outputDir, flowcell.getName());
                logger.debug("path: {}", path);

                for (Sample sample : sampleList) {

                    if ("Undetermined".equals(sample.getBarcode())) {
                        continue;
                    }

                    logger.debug("{}", sample.toString());

                    Set<FileData> fileDataSet = sample.getFileDatas();
                    Set<FileData> fastqFileDataSet = new HashSet<FileData>();

                    for (FileData fileData : fileDataSet) {
                        logger.debug("fileData: {}", fileData);
                        if (MimeType.FASTQ.equals(fileData.getMimeType()) && fileData.getName().endsWith("fastq.gz")
                                && fileData.getPath().startsWith(path)) {
                            fastqFileDataSet.add(fileData);
                        }
                    }

                    File laneBarcodeFastqDirectory = new File(path, String.format("L%03d_%s", sample.getLaneIndex(), sample.getBarcode()));
                    List<String> laneBarcodeFastqDirectoryList = Arrays.asList(laneBarcodeFastqDirectory.list());

                    File nameFastqDirectory = new File(path, sample.getName());
                    List<String> nameFastqDirectoryList = Arrays.asList(nameFastqDirectory.list());

                    String read1 = String.format("%s_%s_L%03d_R%d.fastq.gz", flowcell.getName(), sample.getBarcode(), sample.getLaneIndex(),
                            1);

                    // delete these files from the fs & from the db
                    for (FileData fileData : fastqFileDataSet) {
                        if (fileData.getPath().equals(nameFastqDirectory.getAbsolutePath()) && fileData.getName().equals(read1)
                                && laneBarcodeFastqDirectoryList.contains(read1) && nameFastqDirectoryList.contains(read1)) {
                            Iterator<FileData> iter = fileDataSet.iterator();
                            while (iter.hasNext()) {
                                FileData fd = iter.next();
                                if (fd.equals(fileData)) {
                                    File file2Delete = new File(fd.getPath(), fd.getName());
                                    logger.info("deleting: {}", file2Delete.getAbsolutePath());
                                    try {
                                        FileUtils.forceDelete(file2Delete);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                    iter.remove();
                                }
                            }
                            try {
                                maPSeqDAOBeanService.getSampleDAO().save(sample);
                            } catch (MaPSeqDAOException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    String read2 = String.format("%s_%s_L%03d_R%d.fastq.gz", flowcell.getName(), sample.getBarcode(), sample.getLaneIndex(),
                            2);

                    // delete these files from the fs & from the db
                    for (FileData fileData : fastqFileDataSet) {
                        if (fileData.getPath().equals(nameFastqDirectory.getAbsolutePath()) && fileData.getName().equals(read2)
                                && laneBarcodeFastqDirectoryList.contains(read2) && nameFastqDirectoryList.contains(read2)) {
                            for (Iterator<FileData> iter = fileDataSet.iterator(); iter.hasNext();) {
                                FileData fd = iter.next();
                                if (fd.equals(fileData)) {
                                    File file2Delete = new File(fd.getPath(), fd.getName());
                                    logger.info("deleting: {}", file2Delete.getAbsolutePath());
                                    try {
                                        FileUtils.forceDelete(file2Delete);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                    iter.remove();
                                }
                            }
                            try {
                                maPSeqDAOBeanService.getSampleDAO().save(sample);
                            } catch (MaPSeqDAOException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    // // move file to appropriately named directory & save in db
                    // for (FileData fileData : fastqFileDataSet) {
                    //
                    // if (fileData.getPath().equals(nameFastqDirectory.getAbsolutePath())
                    // && !laneBarcodeFastqDirectory.exists()) {
                    // File srcFile = new File(fileData.getPath(), fileData.getName());
                    // logger.info("moving {} to {}", srcFile.getAbsolutePath(),
                    // laneBarcodeFastqDirectory.getAbsolutePath());
                    // try {
                    // FileUtils.moveFileToDirectory(srcFile, laneBarcodeFastqDirectory, true);
                    // } catch (IOException e1) {
                    // e1.printStackTrace();
                    // }
                    // fileData.setPath(laneBarcodeFastqDirectory.getAbsolutePath());
                    // try {
                    // fileDataDAO.save(fileData);
                    // } catch (MaPSeqDAOException e) {
                    // e.printStackTrace();
                    // }
                    // }
                    //
                    // }

                }

            }

        }

    }

    public MaPSeqDAOBeanService getMaPSeqDAOBeanService() {
        return maPSeqDAOBeanService;
    }

    public void setMaPSeqDAOBeanService(MaPSeqDAOBeanService maPSeqDAOBeanService) {
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
    }

    public List<Long> getFlowcellIdList() {
        return flowcellIdList;
    }

    public void setFlowcellIdList(List<Long> flowcellIdList) {
        this.flowcellIdList = flowcellIdList;
    }

}
