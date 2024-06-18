# STA File Schema

This documentation provides an overview of the schema used in STA files. Different manufacturers have slightly different formats with many similarities. Here, we will describe the data format used by many manufacturers, and how we integrate that into a unified schema that is produced in the output Parquet file.

## File Structure

Many, if not all, manufacturers collect data in proprietary file formats which cannot be easily accessed by external programs. Fortunately, or perhaps necessarily, manufacturers provide output from their proprietary programs into a more user-friendly format. These are often text-based files in the format of '.txt' or '.csv' that contain columns delimited by some character.

The files generally consists of two main sections: the header and the data.

### Header

The header section contains metadata about the file, such as the date performed, sample information, calibration information, etc. This is the primary section acted on by these scripts because they are frequently slightly different between manufacturers and contain different amounts and types of information. In many cases these differing field names correspond to the same information and it is the purpose of these scripts to, when possible, consolidate these fields under one title. This is the information which is contained in the file-wide metadata associated with the output Parquet files.

#### Netzsch Header

Here are the commonly found metadata fields in the Netzsch instrument file header:

# STA File Schema

This documentation provides an overview of the schema used in STA files. Different manufacturers have slightly different formats with many similarities. Here, we will describe the data format used by many manufacturers, and how we integrate that into a unified schema that is produced in the output Parquet file.

## File Structure
Many, if not all, manufacturers collect data in proprietary file formats which cannot be easily accessed by external programs. Fortunately, or perhaps necessarily, manufacturers provide output from their proprietary programs into a more user-friendly format. These are often text-based files in the format of '.txt' or '.csv' that contain columns delimited by some character. The files generally consist of two main sections: the header and the data.

### Header
The header section contains metadata about the file, such as the date performed, sample information, calibration information, etc. This is the primary section acted on by these scripts because they are frequently slightly different between manufacturers and contain different amounts and types of information. In many cases, these differing field names correspond to the same information, and it is the purpose of these scripts to, when possible, consolidate these fields under one title. This is the information which is contained in the file-wide metadata associated with the output Parquet files.

#### Netzsch Header
Here are the commonly found metadata fields in the Netzsch instrument file header:

| Original Name         | Metadata Name | Description | Example |
|-----------------------|---------------|-------------|---------|
| EXPORTTYPE            | exporttype    | Provides the type of export that was performed from the Netzsch Proteus software. | "DATA ALL" |
| FILE                  | file          | Lists the manufacturer data file from which this file was produced. | "DF_FILED_DES_STA_N2_10K_231028_R1.ngb-ss3" |
| FORMAT                | format        | Provides the export format of the file. | "NETZSCH5" |
| FTYPE                 | file_type     | Provides the encoding type of the file. | "ANSI" |
| IDENTITY              | identity      | Provides a user-entered quantity during test setup. | "DF_FILED" |
| DECIMAL               | decimal       | Provides the decimal character. | "POINT" |
| SEPARATOR             | delimiter     | Provides the delimiter used in the file. | "COMMA" |
| MTYPE                 | measurement_type | Provides the measurement type exported. | "DSC" |
| INSTRUMENT            | instrument    | Specifies the name or model of the instrument used for the measurement. | "NETZSCH STA 449F3" |
| PROJECT               | project       | Provides user-input information about the project or experiment associated with the data. | "Oxidative Pyrolysis" |
| DATE/TIME             | date_performed | Indicates the date and time when the measurement was performed. | "2/11/2024 13:12:51 (UTC-5)" |
| CORR. FILE            | correction_file | Refers to the correction file used during the measurement, if any. | |
| TEMPCAL               | temperature_calibration | Provides the temperature calibration file used for the measurement. | "30-01-2024 15:52" |
| SENSITIVITY           | sensitivity_calibration | Provides the sensitivity calibration file used for the measurement. | "30-01-2024 15:52" |
| LABORATORY            | laboratory    | Indicates the laboratory or facility where the measurement took place. | "UL FSRI" |
| OPERATOR              | operator      | Specifies the user-input name or identifier of the operator who performed the measurement. | "Grayson" |
| REMARK                | comments      | Provides any additional remarks or comments related to the measurement. | "Douglas fir, Filed, Closed pan, Pt, Kinetics validation run" |
| SAMPLE                | sample        | User-input name of the sample being tested. | "DF_FILED" |
| SAMPLE MASS           | sample_mass   | Specifies the user-input mass of the sample. | "3.99" |
| MATERIAL              | material      | Indicates the user-input material of the sample. | "Douglas Fir" |
| REFERENCE             | reference     | Refers to a user-input reference material used for comparison or calibration. Often None. | |
| REFERENCE MASS        | reference_mass | Specifies the user-input mass of the reference material. Often 0. | "0" |
| TYPE OF CRUCIBLE      | crucible_type | Describes the user-input type or material of the crucible used. Includes volume and presence of a lid. | "PtRh20 85 µl, with lid" |
| SAMPLE CRUCIBLE MASS  | sample_crucible_mass | Specifies the user-input mass of the crucible containing the sample. | "254.00" |
| REFERENCE CRUCIBLE MASS | reference_crucible_mass | Specifies the user-input mass of the crucible containing the reference material. | "254.04" |
| PURGE {X} MFC           | purge_{x}_mfc   | Provides information about the one of the purge gas mass flow controller. | "NITROGEN,250.0 ml/min" |
| PROTECTIVE MFC        | protective_mfc | Provides information about the protective gas mass flow controller. | "NITROGEN,250.0 ml/min" |
| DSC RANGE             | dsc_range     | Describes the range of the differential scanning calorimetry (DSC) measurement. | "5000" |
| TG RANGE              | tg_range      | Specifies the range of the thermogravimetric analysis (TG) measurement. | "35000" |
| TAU-R                 | tau_r         | Indicates if Tau-R mode was used during the measurement. | "---" |
| CORR. CODE            | correction_code | Not really sure what this represents at the moment. For all of our files it is "000". | "000" |
| EXO                   | exothermic    | Negative or positive 1 value that represents the direction corresponding to exothermic DSC phenomena. | "-1" |
| RANGE                 | range         | Describes the range of temperatures and/or heating rates during this measurement. | "25°C....700°C/0.0....40.0K/min" |
| SEGMENT               | segment       | Specifies the segment number(s) presented and the total number of segments in the temperature program. | "S1-9/9" |
| SEG. {X}                 | segment_{x}     | Additional segment information including start and end temperatures and time or heating rate between them. | "25°C/20.0(K/min)/250°C" |

### Data

The data section contains the actual data in the form of rows and columns. The data is typically organized in a tabular format, with each column separated by a delimiter. This data is generally similar between manufacturers and contains columns such as 'Temperature', 'Time', 'Mass', etc. Slightly different column names (for instance, 'Temp.' vs ''Temperature') are resolved with mappings to a common format. Units are often included in either the column names or the above described header. This information is extracted and stored in the column-based metadata in the output Parquet file.

## Data Schema Definition

The data schema of the STA file is defined by the columns present in the data section. Each column has a name and a data type associated with it. The data types are primarily 'floats' which represent some measured quantity but can occasionally be 'ints' (for example, when logging the current segment number).

The schema definition should include the name and data type of each column, as well as any units that may be present in the parent file.

## Example Data Schema

Here is an example data schema for a basic STA file:

| Column Name  |  Data Type  |  Units |
| -----------  |  ---------  |  ----- |
| Temperature  |  Float      |  '°C'  |
| Time         |  Float      |  'min' |
| Mass         |  Float      |  'mg'  |
| DSC          |  Float      |  'mW'  |
| Segment      |  Int        |  null  |
