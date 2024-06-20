# Cone File Schema

This documentation provides an overview of the schema used in cone calorimeter files. There exists a variety of manufacturers but the one we have and are most familiar with is made by Deatak. It is know that other manufacturers output file formats differ and will be the subject of future work on this project.

## File Structure

The output of the cone calorimeter programs is and excel ('.XLSM') file with two sheets. The first sheet contains the metadata for the test and is denoted 'Scalar Data'. The second sheet contains the actual test data in a typical tabular format with the name 'Scan Data'.

### Header

The metadata sheet contains information about the test, such as the laboratory, sample mass, operator, etc. This is the information which is contained in the file-wide metadata associated with the output Parquet files.

### Data

The data sheet contains the tabular data format. Column names are given on the first row and the next three rows give information about the acquisition parameters. These are dropped from the output as they do not contain useful information. The 4th row contains the units for their respective columns. The baseline row gives information about the state of the values before the test begins and is extracted and given a null time in the output Parquet file. The remainder of the rows contains the actual data and is extracted and stored as-is. Most of the data types are floats except for the 'Start Test' and 'Flame Verif.' indicators which contain a simple binary digit.

The 'Ext Coeff' column is dropped from the data as it is a derived quantity from the 'Smoke Comp' and 'Smoke Meas' columns.
