
import aaf2
import sys 
import glob, os
import aaf2.mxf
import json
import argparse

#globals 

created_file_count = 0
target_filename = None

# functions

def logprint(what):
    if args.debug:
        print (what)

    
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
    logprint("Folderscan done, result:")
    logprint (all_packages)
    return all_packages
       
def process_directory(dir):
    global created_file_count
    #logprint ("Directory mode, searching for video file in " + sys.argv[1], 3)
    packages = find_opatom_files(dir)
    for pack in packages:   
        if (packages[pack]['slotcount'] == len(packages[pack]['files'])):
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
            with aaf2.open(os.path.join(args.odir,args.oname), 'w') as f:
                for _file in packages[pack]['files']:
                    created_file_count += 1 
                    f.content.link_external_mxf(_file)
                    logprint ("Added " + _file)
            checkResult(os.path.join(args.odir,args.oname))
            print ("Created file: " + os.path.join(args.odir,args.oname))     
            args.oname = None # reset oname for next file            
        else:
            logprint("Not yet ready for processing:")
            logprint(packages[pack]['files'])
    sys.exit(0)

def checkResult(_filename):
    try:
        size = os.path.getsize(_filename)
        if (size < 400000):
            raise Exception("Created file [" +_filename+ "] does not have minimum file size of 400kb")
    except (Exception, e):
        print ("Error: " + e)
        sys.exit(1)
        
#MAIN
#commandline arguments
parser = argparse.ArgumentParser(description='AAF File Creator for OPAtom Files')
parser.add_argument('files', metavar='FILES OR FOLDERS', type=str, nargs='+',
                    help='files to add to package (or folder to scan for files)')
parser.add_argument('--debug', help='Enables debugging, example: --debug 1')
parser.add_argument('--odir', help='Sets destination folder for aaf output file (default is same folder as the OPAtom File resides)')
parser.add_argument('--oname', help='Sets destination filename for aaf output file (default is same name as the OPAtom File (.aaf))')
parser.add_argument('--testmode', help='Do not create any file, just output JSON containing found file packages')

args = parser.parse_args()

logprint(args.files)

#process everything


for _item in args.files:
    filemode = None
    if (os.path.isdir(_item)):
        logprint("Detected directory from userinput:" + _item)
        process_directory(_item)
    elif (os.path.isfile(_item)):
        filemode = 1
        logprint("Detected file from userinput: " + _item)
if (filemode):
    if args.odir == None:
        args.odir = os.path.dirname(args.files[0])
    if args.oname == None:
        base=os.path.basename(args.files[0])
        args.oname = os.path.splitext(base)[0] + ".aaf"
    with aaf2.open(os.path.join(args.odir,args.oname), 'w') as f:
        for _file in args.files:
            f.content.link_external_mxf(_file)
            logprint("Added " + _file)
    checkResult(os.path.join(args.odir,args.oname))
    print ("Created file: " + os.path.join(args.odir,args.oname))        
    
    
#todo: check if target file is greater than 111kb
logprint("Done")