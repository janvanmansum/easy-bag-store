Test sets ranges
================

Test sets to test the TarRange class. The file names have the file size
and corresponding number of TAR records (multiples of 512) as a suffix.

For example:

adir_0_1 -> a directory has no file data in the TAR, but one
afile_20_2 -> only a header + 1 record for the file data
bfile_5000_11 -> a header + 10 records for the file data

