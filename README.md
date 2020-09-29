# createaaf
Locates OPAtom mxf files that belong together and creates aaf which contains links to the audio/video opatom files. Mainly designed to be driven by automation tools, especially a commandline executor node in http://ffastrans.com/.

Actually this script is really nothing special, it just utilizes https://github.com/markreidvfx/pyaaf2 to detect which files have the same materialpackage and create a new aaf file using link_external_mxf from pyaaf2.

Using auto-py-to-exe to create the standalone exe file, note that because of that, the exe may be detected as virus on your computer.

Help of the exe file (-h):

<pre>
  usage: createaaf.exe [-h] [--debug DEBUG] [--odir ODIR] [--oname ONAME] [--testmode TESTMODE]
                       FILES OR FOLDERS [FILES OR FOLDERS ...]

  AAF File Creator for OPAtom Files

  positional arguments:
    FILES OR FOLDERS     files to add to package (or folder to scan for files)

  optional arguments:
    -h, --help           show this help message and exit
    --debug DEBUG        Enables debugging, example: --debug 1
    --odir ODIR          Sets destination folder for aaf output file (default is same folder as the OPAtom File resides)
    --oname ONAME        Sets destination filename for aaf output file (default is same name as the OPAtom File (.aaf))
    --testmode TESTMODE  Do not create any file, just output JSON containing found file packages



</pre>
