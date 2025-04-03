/**
 * Represents a single record from the TSV file, containing relevant fields for processing and plotting.
 * This class is used by both TSVProcessor and TSVPlotter to store data extracted from the TSV file.
 */
public class Record {
    String originalFileName;  // Original File.Name (e.g., "20240114_PLC_M_1_Slot2-43_1_16419")
    String fileNameAlias;     // Aliased File.Name (e.g., "PLC_M_1")
    String precursorId;       // Precursor.Id value
    double precursorNormalised; // Precursor.Normalised value
    String gene;              // Gene name

    /**
     * Constructor for a Record object.
     * @param originalFileName The original File.Name (e.g., "20240114_PLC_M_1_Slot2-43_1_16419").
     * @param fileNameAlias The aliased File.Name (e.g., "PLC_M_1").
     * @param precursorId The Precursor.Id value.
     * @param precursorNormalised The Precursor.Normalised value.
     * @param gene The gene name.
     */
    public Record(String originalFileName, String fileNameAlias, String precursorId, double precursorNormalised, String gene) {
        this.originalFileName = originalFileName;
        this.fileNameAlias = fileNameAlias;
        this.precursorId = precursorId;
        this.precursorNormalised = precursorNormalised;
        this.gene = gene;
    }

    // Getter for original File.Name
    public String getOriginalFileName() {
        return originalFileName;
    }

    // Getter for aliased File.Name
    public String getFileNameAlias() {
        return fileNameAlias;
    }
}