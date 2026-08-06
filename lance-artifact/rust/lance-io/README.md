# lance-io

`lance-io` is an internal sub-crate, containing various utilities for
reading and writing data.  It includes reader/writer traits that
define what Lance expects from a filesystem, encoders and decoders to
convert to/from Arrow data and various layouts, and misc. utilities such
as routines for reading protobuf data from files.

**Important Note**: This crate is **not intended for external usage**.
