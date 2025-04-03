import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicInteger; // Added for AtomicInteger

public class TSVProcessor extends RecursiveAction {
    private final long start;               // Start position in the file for this chunk
    private final long end;                 // End position in the file for this chunk
    private final RandomAccessFile file;    // RandomAccessFile for reading the TSV file
    private final ArrayList<Record> records; // List to store extracted records
    private final int geneColIdx;           // Column index for "Genes"
    private final int fileNameColIdx;       // Column index for "File.Name"
    private final int precursorIdColIdx;    // Column index for "Precursor.Id"
    private final int precursorNormColIdx;  // Column index for "Precursor.Normalised"
    private final String targetGene;        // Target gene to filter records
    private final Map<String, String> fileNameToAlias; // Mapping of original File.Name to alias
    private final Map<String, Integer> aliasCount; // Count of aliases for handling duplicates
    private static final int THRESHOLD = 1024 * 1024; // Threshold for chunk size (1MB)
    private final AtomicInteger debugRecordCount = new AtomicInteger(0); // Counter for debugging records

    /**
     * Constructor for TSVProcessor.
     * @param start Start position in the file for this chunk.
     * @param end End position in the file for this chunk.
     * @param file RandomAccessFile for reading the TSV file.
     * @param records List to store extracted records.
     * @param geneColIdx Column index for "Genes".
     * @param fileNameColIdx Column index for "File.Name".
     * @param precursorIdColIdx Column index for "Precursor.Id".
     * @param precursorNormColIdx Column index for "Precursor.Normalised".
     * @param targetGene Target gene to filter records.
     * @param fileNameToAlias Mapping of original File.Name to alias.
     * @param aliasCount Count of aliases for handling duplicates.
     */
    public TSVProcessor(long start, long end, RandomAccessFile file, ArrayList<Record> records,
                       int geneColIdx, int fileNameColIdx, int precursorIdColIdx, int precursorNormColIdx,
                       String targetGene, Map<String, String> fileNameToAlias, Map<String, Integer> aliasCount) {
        this.start = start;
        this.end = end;
        this.file = file;
        this.records = records;
        this.geneColIdx = geneColIdx;
        this.fileNameColIdx = fileNameColIdx;
        this.precursorIdColIdx = precursorIdColIdx;
        this.precursorNormColIdx = precursorNormColIdx;
        this.targetGene = targetGene;
        this.fileNameToAlias = fileNameToAlias != null ? fileNameToAlias : new HashMap<>(); // Ensure initialization
        this.aliasCount = aliasCount != null ? aliasCount : new HashMap<>(); // Ensure initialization
    }

    /**
     * Main computation method for the RecursiveAction.
     * Splits the chunk into smaller sub-chunks if above the threshold, otherwise processes the chunk.
     */
    @Override
    protected void compute() {
        if (end - start <= THRESHOLD) {
            processChunk();
        } else {
            long mid = start + (end - start) / 2;
            // Adjust mid to the nearest newline to avoid splitting a line
            try {
                file.seek(mid);
                while (mid < end && file.readByte() != '\n') {
                    mid++;
                }
                if (mid < end) mid++; // Move past the newline
            } catch (IOException e) {
                throw new RuntimeException("Error adjusting chunk boundary: " + e.getMessage(), e);
            }

            TSVProcessor left = new TSVProcessor(start, mid, file, records, geneColIdx, fileNameColIdx,
                                                precursorIdColIdx, precursorNormColIdx, targetGene, fileNameToAlias, aliasCount);
            TSVProcessor right = new TSVProcessor(mid, end, file, records, geneColIdx, fileNameColIdx,
                                                 precursorIdColIdx, precursorNormColIdx, targetGene, fileNameToAlias, aliasCount);
            invokeAll(left, right);
        }
    }

    /**
     * Processes a single chunk of the TSV file.
     * Reads the chunk into a string, splits it into lines, and extracts records matching the target gene.
     * If targetGene is null, loads all records.
     */
    private void processChunk() {
        StringBuilder chunk = new StringBuilder();
        try {
            file.seek(start);
            long pos = start;

            // Move to the start of a line if not at the beginning
            if (pos > 0) {
                while (pos < end && file.readByte() != '\n') {
                    pos++;
                }
            }

            // Read the chunk into a string
            byte[] buffer = new byte[8192];
            int bytesRead;
            while (pos < end && (bytesRead = file.read(buffer)) != -1) {
                long newPos = pos + bytesRead;
                if (newPos > end) {
                    bytesRead = (int) (end - pos);
                }
                chunk.append(new String(buffer, 0, bytesRead));
                pos += bytesRead;
            }

            // Process lines
            String[] lines = chunk.toString().split("\n");
            for (String line : lines) {
                if (line.trim().isEmpty()) continue; // Skip empty lines

                String[] fields = line.split("\t");
                if (fields.length <= geneColIdx) continue; // Skip incomplete rows

                String gene = fields[geneColIdx];
                if (targetGene != null && !gene.equals(targetGene)) continue; // Skip non-matching genes if targetGene is specified

                String originalFileName = (fields.length > fileNameColIdx) ? fields[fileNameColIdx] : "";
                String baseAlias = TSVPlotter.extractBaseFilename(originalFileName); // Base alias computed by extractBaseFilename

                // Compute the final alias with suffix if needed
                String alias;
                synchronized (fileNameToAlias) {
                    if (fileNameToAlias == null || aliasCount == null) {
                        throw new IllegalStateException("fileNameToAlias or aliasCount is not initialized");
                    }
                    // Check if this exact File.Name has already been mapped
                    if (fileNameToAlias.containsKey(originalFileName)) {
                        alias = fileNameToAlias.get(originalFileName);
                    } else {
                        // Check if the base alias has been used before
                        int count = aliasCount.getOrDefault(baseAlias, 0);
                        if (count > 0) {
                            alias = baseAlias + "(" + count + ")";
                        } else {
                            alias = baseAlias;
                        }
                        aliasCount.put(baseAlias, count + 1);
                        fileNameToAlias.put(originalFileName, alias);
                    }
                }

                int count = debugRecordCount.getAndIncrement();
                if (count < 100) {
                    System.out.println("TSVProcessor - Record " + (count + 1) + ": Original File.Name: " + originalFileName + " -> Alias: " + alias);
                }

                String precursorId = (fields.length > precursorIdColIdx) ? fields[precursorIdColIdx] : "";
                double precursorNormalised = 0.0;
                try {
                    precursorNormalised = (fields.length > precursorNormColIdx) ? 
                        Double.parseDouble(fields[precursorNormColIdx]) : 0.0;
                } catch (NumberFormatException e) {
                    precursorNormalised = 0.0;
                }

                synchronized (records) {
                    records.add(new Record(originalFileName, alias, precursorId, precursorNormalised, gene));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error processing chunk: " + e.getMessage(), e);
        }
    }
}