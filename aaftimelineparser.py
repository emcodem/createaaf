
####
#  aaftimelineparser.py
# emcdem@ffasatrans.com
# initial commit: 24.10.2025
# License: GPL or the one that comes closest to GPL that the used libraries allow
# Description: parses aaf timeline, can use bmxtranswrap to create a consolidated copy of the pieces in the timeline
####
#core modules
import argparse
import sys
import os
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import url2pathname
from urllib.parse import urlparse,unquote
import traceback
import types
from shutil import copy2
#custom modules

from inspect import currentframe, getframeinfo
filename = getframeinfo(currentframe()).filename 
parent = str(Path(filename).resolve().parent)

sys.path.append(parent) #for current path
from aaf_helpers.aafhelpers import mxf_deep_search_by_key # important order of sys paths here, aafhelpers must work with our custom aaf2!
from aaf_helpers.avid_lut import attachLUT

sys.path.append(str(Path.joinpath(Path(parent), "helpers")))
from helpers import exec_ffprobe

sys.path.append(os.path.join(os.path.dirname(__file__), "venv/Lib/site-packages/")) #for e.g. OTIO
import aaf2
import opentimelineio as otio
from opentimelineio.media_linker import MediaLinker
from opentimelineio.schema import ExternalReference
from pymediainfo import MediaInfo

args = None
for name in ["aaf2"]:
    logging.getLogger(name).setLevel(logging.WARNING)
logging.basicConfig(
    level=logging.DEBUG,                   # minimum level to log
    format="%(asctime)s [%(levelname)s] %(message)s"
)

@dataclass
class CutClip:
    path: Path
    start: float
    duration: float
    bmx_start_frames: int = 0
    bmx_duration_frames: int = 0

class BMXCmd():
    def __init__(self, cmd, output):
        self.cmd = cmd
        self.output = output

class CutClipList(list):
    def append(self, item: CutClip):
        # checks if path is already in list, if yes expand start and duration 
                
        if not isinstance(item, CutClip):
            raise TypeError(
                f"Only CutClip instances can be appended, got {type(item).__name__}"
            )

        # Search for an existing clip with the same path
        # for existing in self:
        #     if existing.path == item.path:
        #         existing_end = existing.start + existing.duration
        #         new_end = item.start + item.duration

        #         # Update start if new item starts earlier
        #         if item.start < existing.start:
        #             existing.start = item.start
        #             logging.debug("Updating start %s to: %i",item.path,existing.start)
                    
        #         # Update duration if new item ends later
        #         if new_end > existing_end:
        #             existing.duration = new_end - existing.start
        #             logging.debug("Updating duration %s to: %i",item.path,existing.duration)

        #         # Don’t append a duplicate
        #         return
            
        # No existing clip with same path
        super().append(item)

def run_command(cmd):
    """
    Runs a command, captures stdout/stderr, and returns a dict with all info.
    """
    try:
        result = subprocess.run(
            cmd,
            shell=True,               # use True for cross-platform shell commands
            stdout=subprocess.PIPE,   # capture stdout
            stderr=subprocess.PIPE,   # capture stderr
            text=True                 # return strings instead of bytes
        )
        return {
            "cmd": cmd,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "success": result.returncode == 0
        }
    except Exception as e:
        return {
            "cmd": cmd,
            "returncode": -1,
            "stdout": "",
            "stderr": str(e),
            "success": False
        }


# on some python interpreters, pkg_resources is not available
try:
    import pkg_resources
except ImportError:
    pkg_resources = None

__doc__ = """ Python wrapper around OTIO to convert timeline files between \
formats.

Available adapters: {}
""".format(otio.adapters.available_adapter_names())

def _parsed_args():
    """ parse commandline arguments with argparse """

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-i',
        '--input',
        type=str,
        required=True,
        help='path to input file',
    )
    parser.add_argument(
        '-o',
        '--output',
        type=str,
        required=True,
        help='path to output file',
    )
    parser.add_argument(
        '-s',
        '--source',
        type=str,
        required=False,
        help='folder containing files. The timeline will output only a media name like "filename". We are looking for "filename.mxf" in this source path',
    )
    parser.add_argument(
        '-bmx',
        '--bmx',
        type=str,
        required=False,
        help='path to bmx executable, e.g. c:\\temp\\bmxtranswrap.exe',
    )
    parser.add_argument(
        '-ha',
        '--handle',
        type=int,
        default=50, 
        required=False,
        help='for bmx command, add this amount of frames before and after each partial to restore',
    )
    result = parser.parse_args()
    logging.debug("Input arguments: %s",result)
    
    return result

def ensure_two_backslashes(s: str) -> str:
    if not s.startswith('\\\\'):
        # Remove any existing leading backslashes and then add two
        s = s.lstrip('\\')
        s = '\\\\' + s
    return s

def parseLocatorFromAAF():
    locator_urls = []
    with aaf2.open(args.input) as f:
        for mob in f.content.mobs:
            if isinstance(mob, aaf2.mobs.SourceMob):
                #check if there is video
                if isinstance(mob.descriptor, aaf2.essence.CDCIDescriptor):
                    #find network Locator
                    for locator in mob.descriptor.locator:
                        #p = urlparse(locator.getvalue("URLString"))
                        #file_path = url2pathname(p.path)
                        # while file_path.startswith('\\') and ':' in file_path:
                        #     file_path = file_path[1:]
                        # if (':' not in file_path):
                        #     file_path = ensure_two_backslashes(file_path)
                        file_path = getPathFromNetworkLocator(locator)
                        locator_urls.append(file_path)
    return locator_urls

def getPathFromNetworkLocator(locator):
    p = None
    if type(locator) == aaf2.core.AAFObject:
        #assumes aaf object Networklocator
        p = urlparse(locator.getvalue("URLString"))
    else:
        #assumes mxf object Networklocator
        p = urlparse(locator.data["URLString"])
    file_path = ""
    if p.netloc == '':
        file_path = unquote(p.path)
        #local path, if windows (colon), apply some normalization
        while file_path.startswith("/") and ":" in file_path:
            file_path = file_path[1:]
        file_path = file_path.replace('/', '\\')
    else:
        #UNC path
        file_path = f"\\\\{unquote(p.netloc)}{unquote(p.path)}".replace('/', '\\')
    return file_path
    
def parseLocatorFromMXF(proxy_mxf_path):
    #parse the mxf and find the network locator - if any
    #the usecase for this is that the parsed aaf may contain only proxy mxf, these could potentially carry hints about the location of original essence
    found_paths = []
    m = aaf2.mxf.MXFFile(proxy_mxf_path)
    for _obj in m.objects.items():
        try:
            if isinstance(_obj[1], aaf2.mxf.MXFImportDescriptor):
                _loc = m.objects[_obj[1].data["Locator"][0]]
                if _loc:
                    logging.debug("Found path in MXF Locator (%s): [%s]",proxy_mxf_path,_loc.data["URLString"])
                    file_path = getPathFromNetworkLocator(_loc)
                    logging.debug("Normalized path in MXF Locator (%s): [%s]",proxy_mxf_path,file_path)
                    found_paths.append(file_path)
                    
        except Exception:
            traceback.print_exc()

    return found_paths

def _resolve_media(path,trackname,searchpaths = []):
    #returns either trackname or if a file was found, the full path
    # if path is None:
    #     return trackname

    #try to find trackname in previously parsed locators from proxy mxf
    for _spath in searchpaths:
        if _spath.lower().endswith((f"{trackname.lower()}.mxf", f"{trackname.lower()}.mp4", f"{trackname.lower()}")):
            logging.debug(f"Found {trackname} in Path analyzed from proxy mxf: {_spath}")
            return _spath

    #fall back to the provided search path if any
    logging.error(f"Could not find {trackname} in proxy mxf pathlist.")
    if (path == None):
        raise Exception("Could not find media file for clip: {}".format(trackname))
    
    logging.error(f"Trying to find in fallback path: {path}")
    for ext in (".mxf", ".mp4"):
        path = Path(path)
        _current = path / f"{trackname}{ext}"
        if (_current).exists():
            return(_current)
        
    return trackname

def generate_ffconcat(clips):
    lines = ["ffconcat version 1.0"]
    for clip in clips:
        # FFmpeg expects forward slashes even on Windows
        path_str = str(clip.path).replace("\\", "/")
        lines.append(f"file '{path_str}'")
        lines.append(f"inpoint {clip.start}")
        lines.append(f"outpoint {round(clip.duration + clip.start,3)}")
    logging.debug("\n" + "\n".join(lines))
    logging.info("\n" + "\n".join(lines))
    return "\n".join(lines)

def get_source_rate(filepath):
    media_info = MediaInfo.parse(filepath)
    _parsed = float(media_info.video_tracks[0].frame_rate)
    return _parsed

def generate_bmx(clips,output_path,bmxtranswrap):
    #for each clip, generate a bmx command for shell exec
    output_path = Path(output_path)
    _cmds = []
    apply_handle(clips,args.handle)

    for clip in clips:   
        logging.debug("Analyzing colors for clip: %s", clip.path)
        color_args = ""
        try:
            m = aaf2.mxf.MXFFile(str(clip.path))
            m.walker = types.MethodType(mxf_deep_search_by_key, m) #extend the MXFFile Class, we need "self" to work in mxf_deep_search_by_key
            trc = m.walker(search="TransferCharacteristic")
            prim = m.walker(search="ColorPrimaries")
            eq = m.walker(search="CodingEquations")
            
            if (trc):
                color_args += (" --transfer-ch urn:smpte:ul:" + aaf2.mxf.reverse_auid(trc).hex) + " "
            if (prim):
                color_args += (" --color-prim urn:smpte:ul:" + aaf2.mxf.reverse_auid(prim).hex) + " "
            if (eq):
                color_args += (" --coding-eq urn:smpte:ul:" + aaf2.mxf.reverse_auid(eq).hex) + " "

        except Exception:
            logging.warning("Could not analyze colors for clip: %s", clip.path)
        logging.debug("Colors for clip: %s", color_args)
        
        # we consolidate each clip into its separate dir in order to support multiple cuts on a single source
        _out_dir  = output_path / (str(Path(clip.path).stem) + str(clip.bmx_start_frames) + "-" + str(clip.bmx_duration_frames))
        _out_file = _out_dir / Path(clip.path).name
        logging.debug("Creating output directory: %s",_out_dir)
        os.makedirs(_out_dir, exist_ok=True)
        #_orig_rate = get_source_rate(str(clip.path)) # mediainfo todo: add timeline rate as default
        bmxargs = [
                   str (bmxtranswrap) + " -t op1a -o \""+str(_out_file)+"\" --start ",
                   clip.bmx_start_frames," --dur ",
                   clip.bmx_duration_frames,
                   color_args,
                   " \""+str(clip.path)+"\""]
        bmxargs = [str(x) for x in bmxargs]
        bmxargs = " ".join(bmxargs)     
        _cmds.append (BMXCmd(bmxargs,_out_file))
    return (_cmds)

def apply_handle(clips: List[CutClip], handle: int = 0) -> None:
    """
    Modifies start and duration of each CutClip 
    """
    for clip in clips:
        orig_framerate = get_source_rate(str(clip.path))
        start_frames = clip.start * orig_framerate
        duration_frames = clip.duration * orig_framerate
        reduction = min(handle, start_frames)  # cannot reduce below 0
        clip.bmx_start_frames = start_frames - reduction
        clip.bmx_duration_frames = handle + reduction + duration_frames
        clip.bmx_start_frames = round(start_frames - reduction)
        clip.bmx_duration_frames = round(handle + reduction + duration_frames)
        logging.debug("Calculated bmx start and duration: %i, %i", clip.bmx_start_frames, clip.bmx_duration_frames)

def execute_bmx(bmx_cmds: List[BMXCmd]):
    #execute all bmx cmds parallel
    results = []
    with ThreadPoolExecutor() as executor:
        {logging.debug("Executing: " +"\n" + cmd.cmd) for cmd in bmx_cmds}
        future_to_cmd = {executor.submit(run_command, cmd.cmd): cmd.cmd for cmd in bmx_cmds}
        for future in as_completed(future_to_cmd):
            results.append(future.result())
    #check results        
    failed = [r for r in results if not r["success"]]
    if failed:
        logging.error("The following commands failed:")
        for r in failed:
            logging.error(f"- Command: {r['cmd']}")
            logging.error(f"  Return code: {r['returncode']}")
            logging.error(f"  stderr: {r['stderr'].strip()}")
            logging.error("-" * 40)

    if any(not r["success"] for r in results):
        logging.error("One or more BMX consolidate commands failed!")
        sys.exit(2)
    else:
        logging.debug("All BMX consolidate commands succeeded!")
        #collect output paths and generate aaf
        all_outputs = [r.output for r in bmx_cmds]
        return all_outputs

def write_output_aaf(all_outputs):
        return #not used atm. avid showed the files offline until bin was closed and re-opened
        filename_no_ext = Path(args.input).stem
        out_aaf_path = os.path.join(args.output, filename_no_ext + "_consolidated.aaf")
        logging.info("Output aaf: %s", out_aaf_path)
        with aaf2.open(out_aaf_path, 'w') as f:
            for _file in all_outputs:
                meta = ""
                if not str(_file).lower().endswith(".mxf"):
                    #we need the probe only for non mxf because aaf2 does parse mxf natively
                    meta = exec_ffprobe.get_ffprobe_info(str(_file))
                logging.debug("AMA linking file: %s",_file)
                f.content.create_ama_link(str(_file),meta) # todo emcodem: find out why ama linking dont work 
                attachLUT(f,str(_file),"auto")

def copy_files_parallel(file_list, target_dir, max_workers=4):
    logging.debug("Ensure output dir: %s",target_dir)
    os.makedirs(target_dir, exist_ok=True)
    target_dir = Path(target_dir)
    

    def copy_file(src):
        src_path = Path(src)
        dest_path = target_dir / src_path.name
        logging.info(f"Copying {src_path} to {dest_path}")
        copy2(src_path, dest_path)
        return dest_path

    copied_files = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(copy_file, f): f for f in file_list}
        for future in as_completed(futures):
            try:
                copied_files.append(future.result())
            except Exception as e:
                print(f"Failed to copy {futures[future]}: {e}")

    return copied_files

def main():
    """Parse arguments and convert the files."""
    global args
    args = _parsed_args()
    logging.debug("Creating output dir: %s",args.output)
    os.makedirs(args.output, exist_ok=True)
    #todo: searchpaths can contain the original mxf paths, consider them at resolving media
    searchpaths = []
    try:
        locator_urls = parseLocatorFromAAF()
        logging.debug("Found Locators in AAF: %s",locator_urls)
    except Exception as e:
        logging.error(f"Unexprected Error parsing AAF locators: {e}", exc_info=True)
    try:
        for locator_url in locator_urls:
            searchpaths.extend(parseLocatorFromMXF(locator_url))
        if (len(searchpaths) == 0):
            logging.debug("Did not find any locators in the MXF files that were parsed from the AAF")
    except Exception as e:
        logging.error(f"Unexprected Error parsing MXF locators: {e}", exc_info=True)

    in_adapter = otio.adapters.from_filepath(args.input).name

    result_tl = otio.adapters.read_from_file(
        args.input,
        in_adapter,
    )
    ffconcat_clips = []
    bmx_clips = CutClipList()
    copy_clips = []
    for _t in result_tl.tracks:
        for item in _t:
            if (_t.kind != "Video"):
                continue
            if isinstance(item, otio.schema.Clip):
                sr = item.source_range
                # we hacked the original_file_name into the metadata in advanced_authoring_format.py, no clue if we could determine audio/video files from it
                if (item.metadata.get("AAF", {}).get("original_file_name")):
                    item.name = item.metadata["AAF"]["original_file_name"]
                    logging.debug(f"Using original_file_name: {item.name}")
                _path = _resolve_media(args.source,item.name,searchpaths)
                if (_path == item.name):
                    raise Exception("Could not find media for Clip " + item.name)
                logging.debug(f"Working on Clip: {_path}")
                if not (_path.lower().endswith((".mxf"))):
                    logging.debug(f"Detected non mxf file, adding to copy list: {_path}")
                    copy_clips.append(_path)
                    continue
                if sr:
                    logging.debug(f"    Source range: start={sr.start_time.to_seconds()}, duration={sr.duration.to_seconds()}")
                    ffconcat_clips.append(
                        #todo:ffconcat has outpoint, not duration
                        CutClip(path=_path, start=sr.start_time.to_seconds(), duration=sr.duration.to_seconds())
                        )
                    bmx_clips.append(
                        #bmx wants edit units
                        CutClip(path=_path, start=sr.start_time.to_seconds(), duration=sr.duration.to_seconds())
                        )
                    
                else:
                    logging.debug("    Source range: None")

    logging.info(generate_ffconcat(ffconcat_clips))
    bmx_cmds = (generate_bmx(bmx_clips,args.output,args.bmx))
    all_output_files = execute_bmx(bmx_cmds)

    if (len(copy_clips) > 0):
        copied_targets = copy_files_parallel(copy_clips,args.output)
        all_output_files.extend(copied_targets)

    write_output_aaf(all_output_files)


if __name__ == '__main__':
    try:
        main()
    except otio.exceptions.OTIOError as err:
        logging.error("ERROR: " + str(err) + "\n")
        sys.exit(1)