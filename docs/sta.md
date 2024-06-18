# STA File Schema

This documentation provides an overview of the schema used in STA files. Different manufacturers have slightly different formats with many similarities. Here, we will describe the data format used by many manufacturers, and how we integrate that into a unified schema that is produced in the output Parquet file.

## File Structure

Many, if not all, manufacturers collect data in proprietary file formats which cannot be easily accessed by external programs. Fortunately, or perhaps necessarily, manufacturers provide output from their proprietary programs into a more user-friendly format. These are often text-based files in the format of '.txt' or '.csv' that contain columns delimited by some character.

The files generally consists of two main sections: the header and the data.

### Header

The header section contains metadata about the file, such as the date performed, sample information, calibration information, etc. This is the primary section acted on by these scripts because they are frequently slightly different between manufacturers and contain different amounts and types of information. In many cases these differing field names correspond to the same information and it is the purpose of these scripts to, when possible, consolidate these fields under one title. This is the information which is contained in the file-wide metadata associated with the output Parquet files.

### Netzsch Header

Here are the commonly found metadata fields in the Netzsch instrument file header:

- EXPORTTYPE: Provides the type of export that was performed from the Netzsch Proteus software. Example: "DATA ALL"
- FILE: Lists the manufacturer data file from which this file was produced. Example: "DF_FILED_DES_STA_N2_10K_231028_R1.ngb-ss3"
- FORMAT: Provides the export format of the file. Currently we only have experience with "NETZSCH5" but would love to here others experiences. Example: "NETZSCH5"
- FTYPE: Provides the encoding type of the file. Example: "ANSI"
- IDENTITY: Provides a user-entered quantity during test setup. Example: "DF_FILED"
- DECIMAL: Provides the decimal character. Example: "POINT"
- SEPARATOR: Provides the delimiter used in the file. Example: "COMMA"
- MTYPE: Provides the measurement type exported. Note that this does not necessarily preclude the inclusion of other measurement data. Example: "DSC"
- INSTRUMENT: Specifies the name or model of the instrument used for the measurement.
- PROJECT: Provides user-input information about the project or experiment associated with the data.
- DATE/TIME: Indicates the date and time when the measurement was performed.
- CORR. FILE: Refers to the correction file used during the measurement. if any.
- TEMPCAL: Provides the temperature calibration file used for the measurement.
- SENSITIVITY: Provides the sensitivity calibration file used for the measurement.
- LABORATORY: Indicates the laboratory or facility where the measurement took place.
- OPERATOR: Specifies the user-input name or identifier of the operator who performed the measurement.
- REMARK: Provides any additional remarks or comments related to the measurement.
- SAMPLE: User-input name of the sample being tested.
- SAMPLE MASS: Specifies the user-input mass of the sample.
- MATERIAL: Indicates the user-input material of the sample.
- REFERENCE: Refers to a user-input reference material used for comparison or calibration. Often None.
- REFERENCE MASS: Specifies the user-input mass of the reference material. Often 0.
- TYPE OF CRUCIBLE: Describes the user-input type or material of the crucible used. Includes volume and presence of a lid.
- SAMPLE CRUCIBLE MASS: Specifies the user-input mass of the crucible containing the sample.
- REFERENCE CRUCIBLE MASS: Specifies the user-input mass of the crucible containing the reference material.
- PURGE 1 MFC: Provides information about the first purge gas mass flow controller.
- PURGE 2 MFC: Provides information about the second purge gas mass flow controller.
- PROTECTIVE MFC: Provides information about the protective gas mass flow controller.
- DSC RANGE: Describes the range of the differential scanning calorimetry (DSC) measurement.
- TG RANGE: Specifies the range of the thermogravimetric analysis (TG) measurement.
- TAU-R: Indicates if Tau-R mode was used during the measurement.
- CORR. CODE: Note really sure what this represents at the moment. For all of our files it is "000".
- EXO: Negative or positive 1 value that represents the direction corresponding to exothermic DSC phenomena
- RANGE: Describes the range of temperatures and/or heating rates during this measurement.
- SEGMENT: Specifies the segment number(s) presented and the total number of segments in the temperature program.
- SEG.2: Additional segment information including start and end temperatures and time or heating rate between them.

Please note that these fields may vary depending on the instrument and file format used. It's important to consult the instrument's documentation or the file's metadata for accurate field descriptions. These fields are subject the change in the future and possibly between instruments. Unfortunately we only have a sample-size of 1, but we would love to here from others and their files and seek to integrate them into this system.

Here is a mapping table for the names given above in the list and what we will call them in the metadata:

| Original Name         | Metadata Name |
|-----------------------|---------------|
| EXPORTTYPE            | exporttype   |
| FILE                  | file |
| FORMAT                | format |
| FTYPE                 | ftype |
| IDENTITY              | identity |
| DECIMAL               | decimal |
| SEPARATOR             | delimiter |
| MTYPE                 | measurement_type |
| INSTRUMENT            | instrument |
| PROJECT               | project |
| DATE/TIME             | date_performed |
| CORR. FILE            | correction_file |
| TEMPCAL               | temperature_calibration |
| SENSITIVITY           | sensitivity_calibration |
| LABORATORY            | laboratory |
| OPERATOR              | operator |
| REMARK                | comments |
| SAMPLE                | sample |
| SAMPLE MASS           | sample_mass |
| MATERIAL              | material |
| REFERENCE             | reference |
| REFERENCE MASS        | reference_mass |
| TYPE OF CRUCIBLE      | crucible_type |
| SAMPLE CRUCIBLE MASS  | sample_crucible_mass |
| REFERENCE CRUCIBLE MASS | reference_crucible_mass |
| PURGE 1 MFC           | purge_1_mfc |
| PURGE 2 MFC           | purge_2_mfc |
| PROTECTIVE MFC        | protective_mfc |
| DSC RANGE             | dsc_range |
| TG RANGE              | tg_range |
| TAU-R                 | tau_r |
| CORR. CODE            | correction_code |
| EXO                   | exothermic |
| RANGE                 | range |
| SEGMENT               | segment |
| SEG.2                 | segment_2 |

### Data

The data section contains the actual data in the form of rows and columns. The data is typically organized in a tabular format, with each column separated by a delimiter. This data is generally similar between manufacturers and contains columns such as 'Temperature', 'Time', 'Mass', etc. Slightly different column names (for instance, 'Temp.' vs ''Temperature') are resolved with mappings to a common format. Units are often included in either the column names or the above described header. This information is extracted and stored in the column-based metadata in the output Parquet file.

## Data Schema Definition

The data schema of the STA file is defined by the columns present in the data section. Each column has a name and a data type associated with it. The data types are primarily 'floats' which represent some measured quantity but can occasionally be 'ints' (for example, when logging the current segment number).

The schema definition should include the name and data type of each column, as well as any units that may be present in the parent file.

## Example Data Schema

Here is an example data schema for a basic STA file:

```
Column Name    Data Type    Units
-----------    ---------    -----
Temperature    Float        '°C'
Time           Float        'min'
Mass           Float        'mg'
DSC            Float        'mW'
Segment        Int          null
```
