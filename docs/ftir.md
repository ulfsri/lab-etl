# FTIR File Schema
This documentation provides an overview of the schema used in FTIR files. At the moment, this documentation is limited to only Bruker instruments as that as what we have experience with. Accordingly, all of the ingest processes rely on being able to read the binary instrument files directly as that allows us to capture all of the metadata as compared to some exported type. As such, the ingest uses the 'brukeropus' package developed by [joshduran](https://github.com/joshduran/brukeropus) which handles much of the heavy-lifting.

## File Structure

The file structure of the Bruker FTIR files contains metadata that can be accessed as 'parameters' and 'reference parameters'. These are extracted and stored as-is in the file metadata of the Parquet file.

### Data

Multiple sets of data can be contained in the Bruker file, for instance, 'Reflectance', 'Sample Spectrum', and 'Reference Spectrum', that represent the different parts of the measurement that make up a test. We consider there to be three primary quantities that are of most interest to us: 'Reflectance', 'Transmittance', and 'Absorbance'. Consequently we treat these as the main content of the output Parquet file. However, we also try to extract the remaining spectrum when appropriate. Given that each spectrum has its own set of 'x-values' associated with it, we must interpolate the other spectrum to that of the primary quantity. This does result in some modification and loss of data, however, we view this as an acceptable tradeoff to including more information in the Parquet file. The existing files still exist should we want to go back and look at the originals. All of the primary signals, and consequently the interpolated ones, are given on the basis of wavelength, as that is the primary quantity on which we work in our laboratory but the conversion to wavenumber or frequency is trivial.

The datetime of the measurement is taken from the datetime of one of the primary spectrums. The other spectrums have different datetimes as they are conducted at slightly differing moments, however these are dropped from the resulting Parquet file as they are thought to be of limited utility.

# Brukeropus package

Below is just some sample code and explanations for reference and use of the brukeropus package. Deference should be given to the actual documentation, but this gives some common commands that may be useful.

```
    opus_file = read_opus(path)  # Returns an OPUSFile class
    opus_file.print_parameters()  # Pretty prints all metadata in the file to the console
    print(opus_file.data_keys)  # Returns a list of all data keys in the file

    # General parameter metadata
    dict(opus_file.params)

    # Reference parameter metadata
    dict(opus_file.rf_params)

    # Data
    opus_file.all_data_keys # Returns a list of all data keys in the file: ['rf', 'r', 'sm']
    opus_file.r.label # Returns the label of the reflectance spectrum
    opus_file.r.x # Returns the x-axis data for the reflectance spectrum, in whatever units it was saved in, can be queried with opus_file.r.dxu
    opus_file.r.wl # Returns the x-axis data for the reflectance spectrum as wavelength (um)
    opus_file.r.y # Returns the y-axis data for the reflectance spectrum
    opus_file.r.datetime # Returns the date and time of the measurement

    opus_file.iter_all_data() # Returns a generator that yields all data in the file, i.e. iterates through all data keys

    sm: Single-channel sample spectra
    rf: Single-channel reference spectra
    igsm: Sample interferogram
    igrf: Reference interferogram
    phsm: Sample phase
    phrf: Reference phase
    a: Absorbance
    t: Transmittance
    r: Reflectance
    km: Kubelka-Munk
    tr: Trace (Intensity over Time)
    gcig: gc File (Series of Interferograms)
    gcsc: gc File (Series of Spectra)
    ra: Raman
    e: Emission
    pw: Power
    logr: log(Reflectance)
    atr: ATR
    pas: Photoacoustic

    Conversions between parameter names can be found in
    from brukeropus.file.constants import PARAM_LABELS
```
