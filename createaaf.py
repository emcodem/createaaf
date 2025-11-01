#core modules
import os
import sys
import sys 
import glob, os
import json
import argparse
import subprocess

import re
import logging
#logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.DEBUG)
#addon modules (portable)

from inspect import currentframe, getframeinfo
from pathlib import Path

#add module paths to sys path, otherwise portable perl cannot resolve it
filename = getframeinfo(currentframe()).filename
parent = str(Path(filename).resolve().parent)
sys.path.append(parent)
import aaf2
import aaf2.mxf

sys.path.append(str(Path.joinpath(Path(parent), "helpers")))
from helpers import win_argparse
from helpers import exec_ffprobe

sys.path.append(str(Path.joinpath(Path(parent), "aaf_helpers")))
from aaf_helpers.avid_lut import attachLUT

#globals 

created_file_count = 0
target_filename = None
args = None

# logging.debug("sys.argv before parsing: %s", sys.argv)
# sys.argv = [
#     arg[:-1] if arg.endswith('"') and arg[-2] == '\\' else arg
#     for arg in sys.argv
# ]

def sort_filenames_video_first(name):
    #sort filenames list so _vX files go first (video)
    version_pattern = re.compile(r'_v\.?(\d+)', re.IGNORECASE)
    match = version_pattern.search(name)
    if match:
        return int(match.group(1))  # use the version number
    return float('inf')  # files without _vX go at the end

def find_opatom_files(dir):
    
    logging.debug("Scanning for files in " + dir)
    #foreach file in directory, get out Materialpackage ID and look if all needed parts of opatom file are there (video/audio)
    all_packages = {}
    for _file in os.listdir(dir):
        logging.debug ("Processing file " + _file)
        m = None
        try:
            m = aaf2.mxf.MXFFile(os.path.join(dir , _file))
            if m.operation_pattern != "OPAtom":
                raise Exception("can only link OPAtom mxf files")
        except Exception as e:
            logging.debug(_file + " is not an OPAtom mxf file " )
            continue
            
        _this_package = {'slotcount' : 0, 'files':[]}
        _last_uid = None
		#collect all referenced ID's of this file
        for _pkg in (m.material_packages()):#there should be only one of these packages, todo: catch the case
            _last_uid = _pkg.data['MobID']
            _this_package['slotcount'] =  len(_pkg.data['Slots'])

        if not(_last_uid in all_packages):
            all_packages[_last_uid] = _this_package
        all_packages[_last_uid]['files'].append(os.path.join(dir,_file))
        all_packages[_last_uid]['files'].sort(key=sort_filenames_video_first)    
    logging.debug("Folderscan done, result:")
    logging.debug (all_packages)
    return all_packages
       
def process_directory(dir):
    global created_file_count
    global args

    all_files_in_dir = [os.path.join(dir, f) for f in os.listdir(dir)]
    if args.oname == None:
        base=os.path.basename(all_files_in_dir[0])
        args.oname = os.path.splitext(base)[0] + ".aaf"
        logging.debug("Calculated output filename:" + args.oname )

    packages = find_opatom_files(dir)
    if (args.amalink != "1"):
        mxf_files = [f for f in all_files_in_dir if f.lower().endswith(".mxf")]
        if (len(mxf_files) == 0):
            logging.debug("No mxf files found in (use ama if you want to link non mxf)" + dir)
            sys.exit(1)
    
    if (args.allinone):
        if (len(packages) != 0):
            #op-atom files are grouped in packages
                logging.debug("Mode: OP-Atom allinone non ama")
                first_src = None
                with aaf2.open(os.path.join(args.odir,args.oname), 'w') as f:
                    for pack in packages:
                        for _file in packages[pack]['files']:
                            first_src = first_src or _file
                            mobs = f.content.link_external_mxf(_file)
                    attachLUT(f,first_src,args.lut)
                    #attach_SRCFILE(f)

                checkResult(os.path.join(args.odir,args.oname))
                return
        else:
            #check if all inputs are mxf
            mxf_files = [f for f in all_files_in_dir if f.lower().endswith(".mxf")]
            logging.debug("Mode: MXF allinone non ama")
            with aaf2.open(os.path.join(args.odir,args.oname), 'w') as f:
                for _file in all_files_in_dir:
                    mobs = f.content.link_external_mxf(_file)
                attachLUT(f,_file,args.lut)
            checkResult(os.path.join(args.odir,args.oname))
            logging.info("Wrote: " + os.path.join(args.odir,args.oname))
            return
        

    #non allinone mode
    if (len(packages) != 0 and args.amalink != "1"):
        logging.debug("Mode: OP-Atom single file non ama")
        for pack in packages:
            if (args.skipcheck != None or packages[pack]['slotcount'] == len(packages[pack]['files'])):
                if args.odir == None:
                    args.odir = os.path.dirname(packages[pack]['files'][0])
                    logging.debug("Calculated output directory: " + args.odir + " From file: " + packages[pack]['files'][0])

                if(args.testmode): #just output json, do not write aaf
                    logging.debug("TESTMODE, no aaf is created, output is:")
                    print (packages)
                    continue

                #create output AAF
                if (args.oname == None):
                    base=os.path.basename(packages[pack]['files'][0])
                    args.oname = os.path.splitext(base)[0] + ".aaf"
                logging.debug ("Creating " + os.path.join(args.odir,args.oname))
                
                sourcefiles = []
                with aaf2.open(os.path.join(args.odir,args.oname), 'w') as f:
                    for _file in packages[pack]['files']:
                        sourcefiles.append(_file)
                        if (args.allinone):
                            continue
                        created_file_count += 1 
                        mobs = []
                        if args.amalink:
                            meta = probe(_file)
                            mobs = f.content.create_ama_link(_file)#, meta
                        else:
                            mobs = f.content.link_external_mxf(_file)
                        
                        logging.debug ("Added " + _file)

                    #adds descriptive metadata to the aaf, mostly about controlling the avid metadata in the bin
                    attachLUT(f,sourcefiles[0])
                checkResult(os.path.join(args.odir,args.oname))
                print ("Created file: " + os.path.join(args.odir,args.oname))
                args.oname = None # reset oname for next file            
            else:
                logging.debug("Not yet ready for processing, slotcount is " + str(packages[pack]['slotcount']) + " and filecount is " + str(len(packages[pack]['files'])))
                logging.debug(packages[pack]['files'])
    else:
        #non allinone mode, non opatom
        logging.debug("Mode: single files, non op-atom, ama: %s", args.amalink)
        mxf_files = [f for f in all_files_in_dir if f.lower().endswith(".mxf")]
        if (len(mxf_files) == 0 and args.amalink != "1"):
            logging.debug("No mxf files found in (use ama if you want to link non mxf)" + dir)
            sys.exit(1)

        for _file in all_files_in_dir:
            probe = ""    
            if not str(_file).lower().endswith(".mxf"):
                try:
                    probe = exec_ffprobe.get_ffprobe_info(_file)
                except:
                    logging.warning("FFprobe failed for file: " + _file)
                    continue
            with aaf2.open(os.path.join(args.odir,args.oname), 'w') as f:
                
                if (args.amalink == "1"):
                    f.content.create_ama_link(_file,probe)
                else:
                    f.content.link_external_mxf(_file)
                
                attachLUT(f,_file,args.lut)
                logging.debug ("Created " + (os.path.join(args.odir,args.oname)))
        
    sys.exit(0)


def attach_SRCFILE(f):
    return
    all_packages = f.content.mobs
    dict_ = f.dictionary
    materialpackage = [
        pkg for pkg in all_packages 
        if isinstance(pkg, aaf2.mobs.MasterMob) 
    ]
    #https://github.com/markreidvfx/pyaaf2/issues/51
    #n =  f.create.NetworkLocator()
    #n['URLString'].value = ("file://blabla/bla.mxf")
    tag = f.create.TaggedValue("_SRCFILE",  "file:///C%3A/temp/1avid/FX6_EO0002.MXF") #think this is 16bit color value
    materialpackage[0]['MobAttributeList'].append(tag)
    # tag = f.create.TaggedValue("ama_path_HARRY",  n, aaf2.types["NetworkLocator"]) #think this is 16bit color value
    # if len(materialpackage) == 1:
    #     materialpackage[0]['MobAttributeList'].append(tag)
    # portable_obj = dict_.create.ClassDefinition('ConstantValue')
    # portable_inst = portable_obj()
    # portable_inst['Value'].value = 42  # set some value        
    
def checkResult(_filename):
    try:
        size = os.path.getsize(_filename)
        if (size < 400000):
            raise Exception("Created file [" +_filename+ "] does not have minimum file size of 400kb")
    except Exception as e:
        print ("Error: " + e)
        sys.exit(1)
        
def probe(path, show_packets=False):
    p = subprocess.Popen("ffprobe", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = p.communicate()
    if ("ffprobe" not in str(stderr)):
        raise Exception("ffprobe not found")
        sys.exit(1)

    cmd = ["ffprobe", '-of','json','-show_format','-show_streams', path]
    if show_packets:
        cmd.extend(['-show_packets',])
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = p.communicate()
    if p.returncode != 0:
        raise subprocess.CalledProcessError(p.returncode, subprocess.list2cmdline(cmd), stderr)

    return json.loads(stdout.decode('utf8'))

#MAIN
#commandline arguments

#parser = argparse.ArgumentParser(description='AAF File Creator for OPAtom Files')

def setupParser(parser):
    
    parser.add_argument('--debug', help='Enables debugging, example: --debug 1')    
    parser.add_argument('--lut', help='In Avid Colortransformation (lut), default is no lut. Check color_luts.json. Example:slog3_to_709. use auto for analyzing the input mxf trc and match with color_luts.json. This will take the first mxf video file only')
    parser.add_argument('--oname', help='Sets destination filename for aaf output file (default is same name as the OPAtom File (.aaf))', required=True)
    parser.add_argument('--testmode', help='Do not create any file, just output JSON containing found file packages')
    parser.add_argument('--skipcheck', help='Prevent checking if there are as many source files as slots found in the op-atom')
    parser.add_argument('--amalink', help='Create AMA linked aaf (needs ffprobe in PATH)')
    parser.add_argument('--allinone', help='Add all source files to a single aaf, cannot work for ama')
    parser.add_argument('files', metavar='FILES OR FOLDERS', type=str, nargs='+',
                        help='files to add to package (or folder to scan for files)')
    parser.add_argument('--odir', 
                        nargs='?',      # allow partial or split paths
                        help='Sets destination folder for aaf output file (default is same folder as the OPAtom File resides)', 
                        type=str,
                        required=True,)

def main():
    global args
    #parse arguments
    parser = None
    try:
        parser = argparse.ArgumentParser( epilog="create AAF from OPAtom or AMA_linked from other formats")
        setupParser(parser)
        args = parser.parse_args()
        logging.debug("Input arguments: %s",args)
    except:
        #dirty workaround only works outside of vscode, used to workaround python bug where you cannot submit arg like "C:\path\" (last backslash disturbing)
        parser = win_argparse.CustomArgumentParser( epilog="create AAF from OPAtom or AMA_linked from other formats" ) 
        setupParser(parser)
        args = parser.parse_args()
        logging.debug("Input arguments: %s",args)

    #setup logging
    if args.debug:
        logging.info("Setting up debug logs")
        script_dir = Path(__file__).resolve().parent
        log_file = script_dir / "createaaf.log"
        if os.path.exists(log_file) and os.path.getsize(log_file) > 1000000:
            # Truncate (reset) the log file
            open(log_file, "w").close()
        

        # Root logger
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        # ----- Remove any existing handlers first -----
        if logger.hasHandlers():
            logger.handlers.clear()

        # ----- File handler -----
        file_handler = logging.FileHandler(log_file, mode='a')
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # ----- Console handler -----
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # ----- Silence aaf2.cfb if needed -----
        logging.getLogger("aaf2.cfb").setLevel(logging.WARNING)

    else:
        logging.basicConfig(level=logging.INFO)
    #process everything

    for _item in args.files:
        filemode = None
        if (os.path.isdir(_item)):
            logging.debug("Detected directory from userinput:" + _item)
            logging.debug("Ensure output dire exists:" + args.odir)
            os.makedirs(args.odir, exist_ok=True)
            process_directory(_item)
        elif (os.path.isfile(_item)):
            filemode = 1
            logging.debug("Detected file from userinput: " + _item)
    if (filemode):
        if args.odir == None:
            args.odir = os.path.dirname(args.files[0])
        
        os.makedirs(args.odir, exist_ok=True)
        
        if args.oname == None:
            base=os.path.basename(args.files[0])
            args.oname = os.path.splitext(base)[0] + ".aaf"
        if args.amalink:
            for _item in args.files:
                meta = probe(_item)
                with aaf2.open(_item + ".aaf", 'w') as f:
                    mobs = f.content.create_ama_link(_item, meta)
                    source_packages = [pkg for pkg in mobs if isinstance(pkg, aaf2.mobs.SourceMob)]
                    #do we need to attach lut for ama?
                    logging.debug("AMA Added " + _item)
            checkResult(_item + ".aaf")
            print ("Created file: " + _item + ".aaf")
        else:
            for _item in args.files:
                with aaf2.open(os.path.join(args.odir,args.oname), 'w') as f:
                    for _file in args.files:
                        f.content.link_external_mxf(_file)
                        logging.debug("Added " + _file)
                checkResult(os.path.join(args.odir,args.oname))
                print ("Created file: " + os.path.join(args.odir,args.oname))        
        
        
#todo: check if target file is greater than 111kb
logging.debug("Done")

if __name__ == '__main__':
    main()