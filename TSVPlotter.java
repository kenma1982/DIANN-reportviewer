import java.io.*;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger; // Added for AtomicInteger
import java.util.stream.Collectors;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.data.category.DefaultCategoryDataset;
import javax.swing.JFrame;

/**
 * Core class for processing a TSV file and plotting Precursor.Normalised against File.Name for a given gene and Precursor.Id.
 * Loads the entire file into memory once and filters records in memory for subsequent operations.
 * 
 * Dependencies:
 * - Uses TSVProcessor (in TSVProcessor.java) for chunked file processing (optional, not used in optimized mode).
 * - Uses Record (in Record.java) to represent individual records.
 * 
 * Note: TSVProcessor is a stable utility class for file processing and should not be modified.
 * Focus on modifying this class (TSVPlotter) for plotting-related features.
 * The GUI and main method are handled by TSVPlotterGUI (in TSVPlotterGUI.java).
 */
public class TSVPlotter {
    private ArrayList<Record> records = new ArrayList<>(); // List to store all records from the TSV file
    private boolean isFileLoaded = false; // Flag to track if the file has been loaded
    private static final long CHUNK_SIZE = 1024 * 1024 * 1024; // Process in chunks of 1GB (used only in chunked mode)
    private final Map<String, String> fileNameToAlias = new HashMap<>(); // Mapping of original File.Name to alias
    private final Map<String, Integer> aliasCount = new HashMap<>(); // Count of aliases for handling duplicates
    private final AtomicInteger debugRecordCount = new AtomicInteger(0); // Counter for debugging records

    /**
     * Loads the TSV file into memory if not already loaded.
     * If loadAll is true, loads the entire file into memory; if false, uses TSVProcessor for chunked loading.
     * In optimized mode, the file is loaded only once, and subsequent calls do nothing.
     * @param filePath Path to the TSV file.
     * @param loadAll If true, loads the entire file into memory; if false, processes in chunks using TSVProcessor.
     * @throws IOException If there is an error reading the file or required columns are not found.
     */
    public void loadTSV(String filePath, boolean loadAll) throws IOException {
        if (isFileLoaded) {
            // File is already loaded, no need to reload
            return;
        }

        if (loadAll) {
            loadTSVAllInMemory(filePath);
        } else {
            loadTSVChunked(filePath);
        }
        isFileLoaded = true; // Mark the file as loaded
    }

    /**
     * Processes a single line from the TSV file and returns a Record.
     * Unlike the previous version, this method does not filter by gene, as we want to load all records.
     * @param line The line to process.
     * @param geneColIdx Column index for "Genes".
     * @param fileNameColIdx Column index for "File.Name".
     * @param precursorIdColIdx Column index for "Precursor.Id".
     * @param precursorNormColIdx Column index for "Precursor.Normalised".
     * @return A Record object if the line is valid, or null if the line is empty or incomplete.
     */
    private Record processLine(String line, int geneColIdx, int fileNameColIdx, 
                              int precursorIdColIdx, int precursorNormColIdx) {
        if (line.trim().isEmpty()) return null; // Skip empty lines

        String[] fields = line.split("\t");
        if (fields.length <= geneColIdx) return null; // Skip incomplete rows

        String gene = fields[geneColIdx];
        String originalFileName = (fields.length > fileNameColIdx) ? fields[fileNameColIdx] : "";
        String baseAlias = extractBaseFilename(originalFileName); // Base alias computed by extractBaseFilename

        // Compute the final alias with suffix if needed
        String alias;
        synchronized (fileNameToAlias) {
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
            System.out.println("TSVPlotter - Record " + (count + 1) + ": Original File.Name: " + originalFileName + " -> Alias: " + alias);
        }

        String precursorId = (fields.length > precursorIdColIdx) ? fields[precursorIdColIdx] : "";
        double precursorNormalised = 0.0;
        try {
            precursorNormalised = (fields.length > precursorNormColIdx) ? 
                Double.parseDouble(fields[precursorNormColIdx]) : 0.0;
        } catch (NumberFormatException e) {
            precursorNormalised = 0.0;
        }

        return new Record(originalFileName, alias, precursorId, precursorNormalised, gene);
    }


    /**
     * Loads the entire TSV file into memory and processes it in parallel using Java streams.
     * Suitable for systems with large memory (e.g., >90GB) when the file size is manageable (e.g., 5GB).
     * Loads all records, not filtered by gene, to allow in-memory filtering later.
     * @param filePath Path to the TSV file.
     * @throws IOException If there is an error reading the file or required columns are not found.
     */
    private void loadTSVAllInMemory(String filePath) throws IOException {
        // Check file size
        File file = new File(filePath);
        long fileSize = file.length();
        System.out.println("File size: " + (fileSize / (1024 * 1024)) + " MB");
        System.out.println("Loading entire file into memory...");

        // Read the entire file into memory
        ArrayList<String> lines = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        }

        // Process the header to find column indices
        if (lines.isEmpty()) {
            throw new IOException("Empty file");
        }
        String headerLine = lines.get(0);
        String[] headers = headerLine.split("\t");
        final int geneColIdx = Arrays.asList(headers).indexOf("Genes");
        final int fileNameColIdx = Arrays.asList(headers).indexOf("File.Name");
        final int precursorIdColIdx = Arrays.asList(headers).indexOf("Precursor.Id");
        final int precursorNormColIdx = Arrays.asList(headers).indexOf("Precursor.Normalised");

        if (geneColIdx == -1 || fileNameColIdx == -1 || precursorIdColIdx == -1 || precursorNormColIdx == -1) {
            throw new IOException("Required columns not found in TSV");
        }

        // Process the data lines in parallel (skip the header)
        lines.subList(1, lines.size()).parallelStream()
             .map(line -> processLine(line, geneColIdx, fileNameColIdx, precursorIdColIdx, precursorNormColIdx))
             .filter(record -> record != null)
             .forEach(record -> {
                 synchronized (records) {
                     records.add(record);
                 }
             });
    }

    /**
     * Loads the TSV file in chunks using RandomAccessFile and processes each chunk in parallel with TSVProcessor.
     * Suitable for systems with limited memory or very large files.
     * Loads all records, not filtered by gene, to allow in-memory filtering later.
     * @param filePath Path to the TSV file.
     * @throws IOException If there is an error reading the file or required columns are not found.
     */
    private void loadTSVChunked(String filePath) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            long fileSize = file.length();
            long position = 0;

            // Read the header to find column indices
            StringBuilder headerBuilder = new StringBuilder();
            int b;
            while (position < fileSize && (b = file.read()) != '\n') {
                headerBuilder.append((char) b);
                position++;
            }
            String headerLine = headerBuilder.toString();
            String[] headers = headerLine.split("\t");
            int geneColIdx = -1;
            int fileNameColIdx = -1;
            int precursorIdColIdx = -1;
            int precursorNormColIdx = -1;

            // Find column indices
            for (int i = 0; i < headers.length; i++) {
                if (headers[i].equals("Genes")) geneColIdx = i;
                if (headers[i].equals("File.Name")) fileNameColIdx = i;
                if (headers[i].equals("Precursor.Id")) precursorIdColIdx = i;
                if (headers[i].equals("Precursor.Normalised")) precursorNormColIdx = i;
            }

            if (geneColIdx == -1 || fileNameColIdx == -1 || precursorIdColIdx == -1 || precursorNormColIdx == -1) {
                throw new IOException("Required columns not found in TSV");
            }

            // Process the file in chunks using TSVProcessor
            while (position < fileSize) {
                long remaining = fileSize - position;
                long chunkSize = Math.min(remaining, CHUNK_SIZE);
                // Adjust chunkSize to end at a newline
                long chunkEnd = position + chunkSize;
                file.seek(chunkEnd);
                while (chunkEnd < fileSize && file.readByte() != '\n') {
                    chunkEnd++;
                }
                if (chunkEnd < fileSize) chunkEnd++; // Move past the newline

                // Create a TSVProcessor instance to process the chunk
                TSVProcessor processor = new TSVProcessor(position, chunkEnd, file, records, geneColIdx, fileNameColIdx,
                                                         precursorIdColIdx, precursorNormColIdx, null, fileNameToAlias, aliasCount);
                ForkJoinPool pool = new ForkJoinPool();
                pool.invoke(processor);

                position = chunkEnd;
            }
        }
    }

    /**
     * Extracts the base filename from a full file path by removing the path and file extension.
     * For File.Name values in the format [digits]_XXXXXXX_Slotxxxxxxx, extracts the XXXXXXX part
     * as the alias by removing the leading digits and underscore, and the _Slotxxxxxxx suffix.
     * This method is used by both TSVPlotter and TSVProcessor to simplify File.Name values.
     * @param fullPath The full file path (e.g., "D:\\path\\to\\20240114_PLC_M_1_Slot2-43_1_16419.d").
     * @return The alias (e.g., "PLC_M_1").
     */
    public static String extractBaseFilename(String fullPath) {
        // Find the last path separator (works for both \ and /)
        int lastSlash = Math.max(fullPath.lastIndexOf('/'), fullPath.lastIndexOf('\\'));
        String filename = (lastSlash == -1) ? fullPath : fullPath.substring(lastSlash + 1);

        // Remove the file extension
        int lastDot = filename.lastIndexOf('.');
        if (lastDot != -1) {
            filename = filename.substring(0, lastDot);
        }

        // Extract the middle part by removing the leading digits_ prefix and _Slotxxxxxxx suffix
        String baseAlias = filename;
        int firstUnderscore = filename.indexOf('_');
        if (firstUnderscore != -1) {
            // Check if the characters before the first underscore are all digits
            String prefix = filename.substring(0, firstUnderscore);
            boolean isAllDigits = prefix.matches("\\d+"); // Matches one or more digits
            if (isAllDigits) {
                int slotIndex = filename.indexOf("_Slot", firstUnderscore);
                if (slotIndex != -1) {
                    baseAlias = filename.substring(firstUnderscore + 1, slotIndex);
                } else {
                    baseAlias = filename.substring(firstUnderscore + 1);
                }
            }
        }

        return baseAlias;
    }

    /**
     * Retrieves unique Precursor.Id values for a given gene from the loaded records.
     * Filters the in-memory records list based on the gene name.
     * @param gene The gene to filter by.
     * @return A Set of unique Precursor.Id values for the specified gene.
     */
    public Set<String> getUniquePrecursorIds(String gene) {
        return records.stream()
                      .filter(record -> record.gene.equals(gene))
                      .map(record -> record.precursorId)
                      .collect(Collectors.toSet());
    }

    /**
     * Creates a dataset for plotting Precursor.Normalised against File.Name for a given gene, multiple Precursor.Id values,
     * and selected File.Name aliases.
     * Ensures the x-axis includes only the selected File.Name aliases, with Precursor.Normalised set to 0 for missing records.
     * Each Precursor.Id is plotted as a separate series.
     * Uses the aliased File.Name values from the records list.
     * @param gene The gene to filter by.
     * @param precursorIds The list of Precursor.Id values to plot.
     * @param selectedFileNameAliases The list of selected File.Name aliases to plot.
     * @return A DefaultCategoryDataset for use with JFreeChart.
     */
    public DefaultCategoryDataset createDataset(String gene, List<String> precursorIds, List<String> selectedFileNameAliases) {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        // Use only the selected File.Name aliases for the x-axis
        Set<String> allFileNameAliases = new TreeSet<>(selectedFileNameAliases); // TreeSet for sorted order

        // For each Precursor.Id, create a series
        for (String precursorId : precursorIds) {
            // Filter records for the given gene and precursorId
            Map<String, Double> fileNameAliasToValue = new HashMap<>();
            for (Record record : records) {
                if (record.gene.equals(gene) && record.precursorId.equals(precursorId) && 
                    selectedFileNameAliases.contains(record.getFileNameAlias())) {
                    fileNameAliasToValue.put(record.getFileNameAlias(), record.precursorNormalised);
                }
            }

            // Add data to the dataset for this Precursor.Id, ensuring all selected File.Name aliases are included
            for (String fileNameAlias : allFileNameAliases) {
                double value = fileNameAliasToValue.getOrDefault(fileNameAlias, 0.0);
                dataset.addValue(value, precursorId, fileNameAlias); // Use fileNameAlias directly
            }
        }

        return dataset;
    }

    /**
     * Plots a bar chart of Precursor.Normalised against File.Name for a given gene, multiple Precursor.Id values,
     * and selected File.Name aliases.
     * The x-axis includes only the selected File.Name aliases, with Precursor.Normalised set to 0 for missing records.
     * Rotates the x-axis labels to 90 degrees for better readability.
     * Each Precursor.Id is plotted as a separate series.
     * Displays the chart in a new JFrame window.
     * This method is the primary entry point for plotting-related modifications.
     * @param gene The gene to filter by.
     * @param precursorIds The list of Precursor.Id values to plot.
     * @param selectedFileNameAliases The list of selected File.Name aliases to plot.
     * @return The JFrame containing the chart, for use by the GUI.
     */
    public JFrame plotChart(String gene, List<String> precursorIds, List<String> selectedFileNameAliases) {
        DefaultCategoryDataset dataset = createDataset(gene, precursorIds, selectedFileNameAliases);
        JFreeChart chart = ChartFactory.createBarChart(
            "Precursor.Normalised by File.Name for Gene: " + gene,  // Chart title
            "File Name",                                            // X-axis label
            "Precursor Normalised",                                 // Y-axis label
            dataset,                                                // Dataset
            PlotOrientation.VERTICAL,                               // Orientation
            true,                                                   // Show legend
            true,                                                   // Show tooltips
            false                                                   // Show URLs
        );

        // Rotate x-axis labels to 90 degrees for better readability
        CategoryPlot plot = chart.getCategoryPlot();
        CategoryAxis domainAxis = plot.getDomainAxis();
        domainAxis.setCategoryLabelPositions(CategoryLabelPositions.createUpRotationLabelPositions(Math.PI / 2.0)); // 90 degrees

        // Create a chart panel and display it in a JFrame
        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(800, 600));  // Set chart size

        JFrame frame = new JFrame("Chart for Gene: " + gene + ", Precursor.Id: " + String.join(", ", precursorIds));
        frame.setContentPane(chartPanel);
        frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        frame.pack();
        return frame;
    }
	
	    /**
     * Gets the list of all records loaded from the TSV file.
     * @return The list of Record objects.
     */
    public List<Record> getRecords() {
        return records;
    }
	
    /**
     * Gets the mapping of original File.Name to alias.
     * @return The map of original File.Name to alias.
     */
    public Map<String, String> getFileNameToAlias() {
        return fileNameToAlias;
    }	
	
	
}