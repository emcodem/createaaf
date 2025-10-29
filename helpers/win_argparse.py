import sys, os, shlex
import argparse # pip install argparse

#
# CustomArgumentParser on windows takes care about parameters ending wiht \" like "c:\temp\"
# These parameters are not parsed correctly by argparse by default, it treats \" as escape sequennce
# https://stackoverflow.com/questions/1291291/how-to-accept-command-line-args-ending-in-backslash

IS_WINDOWS = sys.platform.startswith( 'win' )
IS_FROZEN  = getattr( sys, 'frozen', False )
    
class CustomArgumentParser( argparse.ArgumentParser ):
    if IS_WINDOWS:
        # override
        def parse_args( self ):
            def rawCommandLine():
                from ctypes.wintypes import LPWSTR
                from ctypes import windll
                Kernel32 = windll.Kernel32
                GetCommandLineW = Kernel32.GetCommandLineW
                GetCommandLineW.argtypes = ()
                GetCommandLineW.restype  = LPWSTR
                return GetCommandLineW()                            
            NIX_PATH_SEP = '/'                
            commandLine = rawCommandLine().replace( os.sep, NIX_PATH_SEP )
            skipArgCount = 1 if IS_FROZEN else 2
            args = shlex.split( commandLine )[skipArgCount:]        
            return argparse.ArgumentParser.parse_args( self, args )

#
# # USAGE
# 
       
# parser = CustomArgumentParser( epilog="DEMO HELP EPILOG" ) 
# parser.add_argument( '-v', '--verbose', default=False, action='store_true', 
#                      help='enable verbose output' )
# parser.add_argument( '-t', '--target', default=None,
#                      help='target directory' )                           
# args = parser.parse_args()                       
# print( "verbose: %s" % (args.verbose,) )
# print( "target: %s" % (os.path.normpath( args.target ),) )