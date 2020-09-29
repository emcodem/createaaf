# createaaf
Locates OPAtom mxf files that belong together and creates aaf which contains links to the audio/video opatom files.
Actually the script is nothing special, it just utilizes https://github.com/markreidvfx/pyaaf2 to detect which files have the same materialpackage and create a new aaf file using link_external_mxf from pyaaf2.

Using auto-py-to-exe to create the standalone exe file, note that because of that, the exe may be detected as virus on your computer.
