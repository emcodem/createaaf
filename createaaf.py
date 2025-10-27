#core modules
import os
import sys
import sys 
import glob, os
import json
import argparse
import subprocess
import types
import re

#addon modules (portable)
from inspect import currentframe, getframeinfo
from pathlib import Path
filename = getframeinfo(currentframe()).filename
parent = str(Path(filename).resolve().parent)
sys.path.append(parent)
import aaf2
import aaf2.mxf
from aafhelpers import mxf_deep_search_by_key

#globals 

created_file_count = 0
target_filename = None

# functions

def logprint(what):
    if args.debug:
        print (what)



def sort_filenames_video_first(name):
    #sort filenames list so _vX files go first (video)
    version_pattern = re.compile(r'_v\.?(\d+)', re.IGNORECASE)
    match = version_pattern.search(name)
    if match:
        return int(match.group(1))  # use the version number
    return float('inf')  # files without _vX go at the end

def find_opatom_files(dir):
    
    logprint("Scanning for files in " + dir)
    #foreach file in directory, get out Materialpackage ID and look if all needed parts of opatom file are there (video/audio)
    all_packages = {}
    for _file in os.listdir(dir):
        logprint ("Processing file " + _file)
        m = None
        try:
            m = aaf2.mxf.MXFFile(os.path.join(dir , _file))
            if m.operation_pattern != "OPAtom":
                raise Exception("can only link OPAtom mxf files")
        except Exception as e:
            logprint(_file + " is not an OPAtom mxf file " )
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
    logprint("Folderscan done, result:")
    logprint (all_packages)
    return all_packages
       
def process_directory(dir):
    global created_file_count
    
    packages = find_opatom_files(dir)
    if (args.allinone):
        first_src = None
        with aaf2.open(os.path.join(args.odir,args.oname), 'w') as f:
            for pack in packages:
                for _file in packages[pack]['files']:
                    first_src = first_src or _file
                    mobs = f.content.link_external_mxf(_file)
            attachLUT(f,first_src)
            #attach_SRCFILE(f)

        checkResult(os.path.join(args.odir,args.oname))
        return
    
    #non allinone mode        
    for pack in packages:   
        if (args.skipcheck != None or packages[pack]['slotcount'] == len(packages[pack]['files'])):
            if args.odir == None:
                args.odir = os.path.dirname(packages[pack]['files'][0])
                logprint("Calculated output directory: " + args.odir + " From file: " + packages[pack]['files'][0])
            if args.oname == None:
                base=os.path.basename(packages[pack]['files'][0])
                args.oname = os.path.splitext(base)[0] + ".aaf"
                logprint("Calculated output filename:" + args.oname )

            if(args.testmode): #just output json, do not write aaf
                logprint("TESTMODE, no aaf is created, output is:")
                print (packages)
                continue
            #create output AAF
            logprint ("Creating " + os.path.join(args.odir,args.oname))
            
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
                    
                    logprint ("Added " + _file)

                #adds descriptive metadata to the aaf, mostly about controlling the avid metadata in the bin

                attachLUT(f,sourcefiles[0])
                #attach_SRCFILE(f)
                # if (args.allinone):
                #     for _file in sourcefiles:
                #         mobs = f.content.link_external_mxf(_file)
                #         logprint ("Added Allinone" + _file)
            checkResult(os.path.join(args.odir,args.oname))
            
            print ("Created file: " + os.path.join(args.odir,args.oname))
            args.oname = None # reset oname for next file            
        else:
            logprint("Not yet ready for processing, slotcount is " + str(packages[pack]['slotcount']) + " and filecount is " + str(len(packages[pack]['files'])))
            logprint(packages[pack]['files'])
    sys.exit(0)

def ensureLUTFile():
    #just ensures the file exists, if not write with defaults
    script_dir = os.path.dirname(os.path.abspath(__file__))
    lut_file = os.path.join(script_dir, "color_luts.json")
    default_luts = [
    {
        "name": "slog3_to_709",
        "trc": "060e2b34040101060e06040101010605",
        "lut": "<ColorTransformationList><Name>From S-Log3 / S-Gamut3.Cine to Rec.709</Name><ColorTransformation><ExternalLut><Name>Sony_S3C 1. SLog3-SG3.Cine to LC-709</Name><LutFileName>SLog3SGamut3.CineToLC-709 (video levels).cube</LutFileName><LutFileFormat>iridas</LutFileFormat><LutDimension>3D</LutDimension></ExternalLut></ColorTransformation></ColorTransformationList>"
    },
    {
        "name": "clog2_to_709",
        "trc": "060e2b340401010d0e15000102000000",
        "lut": "<ColorTransformationList><ColorTransformation><ExternalLut><Name>Canon C-Log to REC709</Name><LutFileName>Canon_CLog2Video_Rec709_iridas1d.txt</LutFileName><LutFileFormat>iridas</LutFileFormat><LutDimension>1D</LutDimension></ExternalLut></ColorTransformation></ColorTransformationList>"
    },
    {
		"comment": "for canon we dont have details yet, we map all found trc ul's to the one and only clog lut of avid",
        "name": "clog3_to_709",
        "trc": "060e2b340401010d0e15000107000000",
        "lut": "<ColorTransformationList><ColorTransformation><ExternalLut><Name>Canon C-Log to REC709</Name><LutFileName>Canon_CLog2Video_Rec709_iridas1d.txt</LutFileName><LutFileFormat>iridas</LutFileFormat><LutDimension>1D</LutDimension></ExternalLut></ColorTransformation></ColorTransformationList>"
    }
]
    if not os.path.exists(lut_file):
        with open(lut_file, "w", encoding="utf-8") as f:
            json.dump(default_luts, f, indent=4)
            print(f"Created default LUT file at {lut_file}")
    return lut_file

def autoLUT(lut_table, existing_mxf_file_path):
    # we choose lut only based on trc, not sure if we really need to consider other params, e.g. does HLG with 709 primaries exist?
    m = aaf2.mxf.MXFFile(existing_mxf_file_path)
    m.walker = types.MethodType(mxf_deep_search_by_key, m) #extend the MXFFile Class, we need "self" to work in mxf_deep_search_by_key
    trc = m.walker(search="TransferCharacteristic")
    if (trc == None):
        logprint("Autolut failed, no trc in " + existing_mxf_file_path)
    trc = aaf2.mxf.reverse_auid(trc).hex
    # prim = m.walker(search="ColorPrimaries")
    # eq = m.walker(search="CodingEquations")
    work_lut = next(
        (entry["lut"] for entry in lut_table if entry["trc"] == trc),
        None
    )
    if (work_lut == None):
        logprint("Autolut failed, no matching LUT found for trc" + trc)
    else:
        logprint("Autolut result: " + work_lut)
    return work_lut



def attachLUT(f,existing_mxf_file_path):
    #ensure color luts file exists
    
    if args.lut == None: #no userinput no work
        return
    
    #cares about LUT mapping file
    lut_file = ensureLUTFile()
    lut_table = {}  
    with open(lut_file, "r", encoding="utf-8") as _tmp:
        lut_table = json.load(_tmp)
    
    all_packages = f.content.mobs
    video_sources = [
        pkg for pkg in all_packages 
        if isinstance(pkg, aaf2.mobs.SourceMob) and isinstance(pkg.descriptor, aaf2.essence.CDCIDescriptor)
    ]

    #select lut from userparam from color_luts.json
    work_lut = None
    if args.lut == "auto": 
        try:
            work_lut = autoLUT(lut_table, existing_mxf_file_path)
        except:
            logprint("Autolut failed, no trc in " + existing_mxf_file_path)
            pass
    else:
        work_lut = next(
            (entry["lut"] for entry in lut_table if entry["name"] == work_lut),
            None
        )

    if (work_lut == None):
        print("LUT not found in color_luts.json:",work_lut)
        return
    
    #do the acutal work
    for src in video_sources:
        tag = f.create.TaggedValue("_COLOR_INPUT_TRANSFORMATION",  work_lut) 
        src['MobAttributeList'].append(tag)
    # if len(video_sources) == 1:
    #     video_sources[0]['MobAttributeList'].append(tag)

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
    except (Exception, e):
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
parser = argparse.ArgumentParser(description='AAF File Creator for OPAtom Files')
parser.add_argument('files', metavar='FILES OR FOLDERS', type=str, nargs='+',
                    help='files to add to package (or folder to scan for files)')
parser.add_argument('--debug', help='Enables debugging, example: --debug 1')
parser.add_argument('--odir', help='Sets destination folder for aaf output file (default is same folder as the OPAtom File resides)')
parser.add_argument('--lut', help='In Avid Colortransformation (lut), default is no lut. Check color_luts.json. Example:slog3_to_709. use auto for analyzing the input mxf trc and match with color_luts.json. This will take the first mxf video file only')

parser.add_argument('--oname', help='Sets destination filename for aaf output file (default is same name as the OPAtom File (.aaf))')
parser.add_argument('--testmode', help='Do not create any file, just output JSON containing found file packages')
parser.add_argument('--skipcheck', help='Prevent checking if there are as many source files as slots found in the op-atom')
parser.add_argument('--amalink', help='Create AMA linked aaf (needs ffprobe in PATH)')
parser.add_argument('--allinone', help='Add all source files to a single aaf, cannot work for ama')
args = parser.parse_args()

logprint(args.files)

#process everything


for _item in args.files:
    filemode = None
    if (os.path.isdir(_item)):
        logprint("Detected directory from userinput:" + _item)
        logprint("Ensure output dire exists:" + args.odir)
        os.makedirs(args.odir, exist_ok=True)
        process_directory(_item)
    elif (os.path.isfile(_item)):
        filemode = 1
        logprint("Detected file from userinput: " + _item)
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
                logprint("AMA Added " + _item)
        checkResult(_item + ".aaf")
        print ("Created file: " + _item + ".aaf")
    else:
        for _item in args.files:
            with aaf2.open(os.path.join(args.odir,args.oname), 'w') as f:
                for _file in args.files:
                    f.content.link_external_mxf(_file)
                    logprint("Added " + _file)
    checkResult(os.path.join(args.odir,args.oname))
    print ("Created file: " + os.path.join(args.odir,args.oname))        
    
    
#todo: check if target file is greater than 111kb
logprint("Done")