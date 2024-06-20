# HFM File Schema

This documentation provides an overview of the schema used in heat flow meter files. There exists a variety of manufacturers but the one we have and are most familiar with is made by Waters/TA.

## File Structure

The output of the heat flow meter programs is and text ('.txt') file. This is possibly the nastiest file we have to deal with and resembles a word document rather than any standardized instrument output. The is a large section of metadata at the top and then also interspersed throughout. It is also questionable whether this should actually be stored in a Parquet file because it is literally like a 5x3 table but we are doing it to maintain consistency. We could move it to a JSON in the future if we wish.

The way that we extract data and metadata from these files right down is extremely manually. We pretty much have to check every line and the text in it to determine what it is. I've developed better solution in the past for a small subset of data with complex regex but it is a pain to read and edit and hard to expand. As such, this is the solution at the moment but I would love a better one.
