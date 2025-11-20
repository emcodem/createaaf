
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
from unittest import result
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
import analyze_mxf_colors

sys.path.append(str(Path.joinpath(Path(parent), "helpers")))
from helpers import exec_ffprobe

# Install packages in venv (after moving the venv path):
# python -m venv --upgrade C:\FFAStrans-Public-1.4.2\avid_tools\0.7\createaaf\venv
# C:\FFAStrans-Public-1.4.2\avid_tools\0.7\createaaf\venv\Scripts\activate
# get-command pip (must be in the venv)
# python -m pip install hachoir
sys.path.append(os.path.join(os.path.dirname(__file__), "venv/Lib/site-packages/")) #for e.g. OTIO
import aaf2
import opentimelineio as otio
from opentimelineio.media_linker import MediaLinker
from opentimelineio.schema import ExternalReference
from pymediainfo import MediaInfo
import xml.etree.ElementTree as ET

args = None
for name in ["aaf2"]:
    logging.getLogger(name).setLevel(logging.WARNING)
logging.basicConfig(
    level=logging.DEBUG,                   # minimum level to log
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class ProcessingCmd():
    def __init__(self, cmd, output):
        self.cmd = cmd
        self.output = output
@dataclass
class CutClip:
    path: str
    start: float
    duration: float
    output_file: Path = None
    output_dir: str = None
    ffmpeg_copy_cmd: ProcessingCmd = None
    bmx_cmd: ProcessingCmd = None
    processing_success: bool = False
    bmx_start_frames: int = 0
    bmx_duration_frames: int = 0

class CutClipList(list):
    #todo: this is a normal list now, could remove it.
    def append(self, item: CutClip):
        if not isinstance(item, CutClip):
            raise TypeError(
                f"Only CutClip instances can be appended, got {type(item).__name__}"
            )
        super().append(item)

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
        '-output-json',
        '--output-json',
        type=str,
        required=False,
        help='Path to output JSON file containing the calculated processing commands. If specified, we skip executing the commands.',
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
        '-ffmpeg',
        '--ffmpeg',
        type=str,
        required=False,
        help='path to ffmpeg executable, e.g. c:\\temp\\ffmpeg.exe',
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

# def generate_ffconcat(clips):
#     lines = ["ffconcat version 1.0"]
#     for clip in clips:
#         # FFmpeg expects forward slashes even on Windows
#         path_str = str(clip.path).replace("\\", "/")
#         lines.append(f"file '{path_str}'")
#         lines.append(f"inpoint {clip.start}")
#         lines.append(f"outpoint {round(clip.duration + clip.start,3)}")
#     logging.debug("\n" + "\n".join(lines))
#     logging.info("\n" + "\n".join(lines))
#     return "\n".join(lines)

def get_source_rate(filepath):
    media_info = MediaInfo.parse(filepath)
    _parsed = float(media_info.video_tracks[0].frame_rate)
    return _parsed

def reversed_timecode(n):
    """
    Convert an integer like 16135218 into SMPTE timecode 18:52:13:16 (sony xml LtcChange is written like that)
    assuming the value is reversed HHMMSSFF.
    """
    s = f"{n:08d}"                 # pad to 8 digits
    pairs = [s[i:i+2] for i in range(0, 8, 2)]
    reversed_pairs = pairs[::-1]    # reverse the pairs
    return ":".join(reversed_pairs)

def timecode_to_frames(tc, fps=25):
    h, m, s, f = map(int, tc.split(":"))
    total_frames = int(((h * 3600 + m * 60 + s) * fps) + f)
    return total_frames

def frames_to_timecode(total_frames, fps=25):
    total_frames = int(total_frames)
    fps_int = int(round(fps))  # Round fps for frame calculations
    frames = total_frames % fps_int
    total_seconds = total_frames // fps_int
    s = total_seconds % 60
    total_minutes = total_seconds // 60
    m = total_minutes % 60
    h = total_minutes // 60
    return f"{h:02d}:{m:02d}:{s:02d}:{frames:02d}"

def generate_ffmpeg_copy_cmds(clips,output_path,ffmpeg_path):
    output_path = Path(output_path)
    lines = []
    for clip in clips:
        # # FFmpeg expects forward slashes even on Windows
        # path_str = str(clip.path).replace("\\", "/")
        try:
            timecode = None
            if (clip.path.lower().endswith(".mp4")):
                result = analyze_mxf_colors.extract_xml_from_sony_mp4(clip.path)
                if result and "xml" in result:
                    for elem in result["xml"].iter():
                        # Ignore namespace by splitting at '}' if it exists
                        tag_name = elem.tag.split('}')[-1]  # This strips the namespace
                        if tag_name == 'LtcChange':
                            timecode = reversed_timecode(int(elem.attrib['value']))
                            if (clip.start > 0):
                                f_fps = get_source_rate(clip.path)
                                tc_frames = timecode_to_frames(timecode, f_fps)
                                tc_frames += round(clip.start * f_fps) # adds start offset
                                timecode = frames_to_timecode(tc_frames, f_fps)
                            _cmd = (f"\"{ffmpeg_path}\"  -i \"{str(clip.path)}\" -ss {clip.start} -t {clip.duration} -timecode {timecode} -map 0:v:0 -map 0:a? -codec copy -y \"{clip.output_file}\"")
                            clip.ffmpeg_copy_cmd = ProcessingCmd(_cmd, clip.output_file)
                            continue
        except Exception as e:
            logging.debug(f"Failed to generate ffmpeg copy command for clip {clip.path}: {e}")
            continue
    
    return

def generate_bmx_cmds(clips,bmxtranswrap):
    #for each clip, generate a bmx command for shell exec
    
    _cmds = []
    for clip in clips:
        _out_dir  = clip.output_file.parent
        _out_file = clip.output_file
        if not (str(_out_file).lower().endswith(".mxf")):
            continue

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
        clip.bmx_cmd = ProcessingCmd(bmxargs,_out_file) 
    return (_cmds)

def apply_handle(clips: List[CutClip], handle: int = 0) -> None:
    """
    Modifies start and duration of each CutClip 
    """
    #todo: apply handle to actual clip.start and duration
    for clip in clips:
        orig_framerate = get_source_rate(str(clip.path))
        start_frames = clip.start * orig_framerate
        duration_frames = clip.duration * orig_framerate
        reduction = min(handle, start_frames)  # cannot reduce below 0
        clip.bmx_start_frames = start_frames - reduction
        clip.bmx_duration_frames = handle + reduction + duration_frames
        clip.bmx_start_frames = round(start_frames - reduction)
        clip.bmx_duration_frames = round(handle + reduction + duration_frames)

        #ffmpeg processing needs the seconds. TODO: should we get rid of calculating bmx_start_frames and let the bmx command creator do it
        clip.start = round(clip.bmx_start_frames / orig_framerate, 3)
        clip.duration = round(clip.bmx_duration_frames / orig_framerate, 3)
        clip.output_file = None
        logging.debug("Calculated bmx start and duration: %i, %i", clip.bmx_start_frames, clip.bmx_duration_frames)

def run_command(cmd:str, clip:CutClip):
    """
    Runs a command, captures stdout/stderr, and returns a dict with all info.
    """
    _out_dir  = clip.output_file.parent
    logging.debug("Creating output directory: %s",_out_dir)
    os.makedirs(_out_dir, exist_ok=True)

    logging.debug("Running command: %s", cmd)

    #create output file path for clip
    try:
        result = subprocess.run(
            cmd,
            shell=True,               # use True for cross-platform shell commands
            stdout=subprocess.PIPE,   # capture stdout
            stderr=subprocess.PIPE,   # capture stderr
            text=True                 # return strings instead of bytes
        )
        clip.processing_success = (result.returncode == 0)
        logging.debug("Command finished with return code %s", result.returncode)
        if result.returncode != 0:
            logging.error("Stdout: %s", result.stdout)
            logging.error("Stderr: %s", result.stderr)
        return {
            "cmd": cmd,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "success": result.returncode == 0
        }
    except Exception as e:
        clip.processing_success = False
        return {
            "cmd": cmd,
            "returncode": -1,
            "stdout": "",
            "stderr": str(e),
            "success": False
        }

def execute_commands(clip_list: List[CutClip],cmd_type):#cmd_type is bmx_cmd or ffmpeg_copy_cmd
    #execute all bmx cmds parallel
    results = []
    
    # Filter clips that have bmx commands
    clips_with_cmd_type = [cmd for cmd in clip_list if getattr(cmd, cmd_type) is not None]
    
    # Log commands to be executed
    for cmd in clips_with_cmd_type:
        logging.debug("Executing: " + "\n" + getattr(cmd, cmd_type).cmd)
    
    with ThreadPoolExecutor() as executor:
        # Submit all commands
        future_to_cmd = {}
        for clip in clips_with_cmd_type:
            future = executor.submit(run_command, getattr(clip, cmd_type).cmd,clip)
            future_to_cmd[future] = getattr(clip, cmd_type).cmd
        
        # Wait for completion and process results
        for future in as_completed(future_to_cmd):
            #check if failed, if yes, execute ffmpeg cmd
            result = future.result()

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

def copy_files_parallel(clip_list:List[CutClip], target_dir, max_workers=4):
    logging.debug("Ensure output dir: %s",target_dir)
    os.makedirs(target_dir, exist_ok=True)
    target_dir = Path(target_dir)
    

    def copy_file(src: CutClip):
        src_path = Path(src.path)
        dest_path = target_dir / src_path.name
        logging.info(f"Copying {src_path} to {dest_path}")
        copy2(src_path, dest_path)
        return dest_path

    copied_files = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(copy_file, f): f for f in clip_list}
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

    #
    # PARSE INPUT AAF SEQUENCE - finds original media from locators in the proxy opatom mxf
    #
    in_adapter = otio.adapters.from_filepath(args.input).name
    result_tl = otio.adapters.read_from_file(
        args.input,
        in_adapter,
    )
    main_clip_list = CutClipList()
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
                if sr:
                    # sad but true, otio is not able to give back the start_time in seconds, it uses the project rate instead of the source rate
                    real_rate = get_source_rate(_path)
                    my_start_time = sr.start_time.value / real_rate
                    my_duration = sr.duration.value / real_rate
                    
                    
                    
                    
                    
                    
                    
                    #this is for most recent otio pyaaf adapter where we calculate with the real timecode not zero tc
                    #my_start_time = sr.start_time.to_seconds() - item.media_reference.available_range.start_time.to_seconds()
                    #y_duration = sr.duration.to_seconds()
                    
                    #sr.start_time.rate = real_rate
                    #sr.duration.rate   = real_rate
                    logging.debug(f"    Source range: start={my_start_time}, duration={my_duration}")
                    main_clip_list.append(
                        #bmx wants edit units
                        CutClip(path=_path, start=my_start_time, duration=my_duration)
                        )
                    
                else:
                    logging.debug("    Source range: None")

    #
    # Processing strategy: first try bmx (on mxf), then for all failed or not yet processed, try ffmpeg
    # as a last resort, if both bmx and ffmpeg failed, just copy the input file.
    #
    
    # applies handle
    apply_handle(main_clip_list,args.handle)

    # calc output file path
    for clip in main_clip_list:
        _out_dir  = Path(args.output) / (str(Path(clip.path).stem) + "_" + str(clip.start) + "-" + str(clip.duration))
        _out_file = _out_dir / Path(clip.path).name
        clip.output_file    = _out_file
        clip.output_dir     = _out_dir

    if (args.output_json):
        generate_bmx_cmds(main_clip_list,args.bmx)
        generate_ffmpeg_copy_cmds(main_clip_list,args.output,args.ffmpeg)
        with open(args.output_json, 'w') as json_file:
            import json
            json_data = []
            for clip in main_clip_list:
                clip_dict = {
                    "path": clip.path,
                    "start": clip.start,
                    "duration": clip.duration,
                    "output_file": str(clip.output_file),
                    "output_dir": str(clip.output_dir),
                    "bmx_cmd": clip.bmx_cmd.cmd if clip.bmx_cmd else None,
                    "ffmpeg_copy_cmd": clip.ffmpeg_copy_cmd.cmd if clip.ffmpeg_copy_cmd else None
                }
                json_data.append(clip_dict)
            json.dump(json_data, json_file, indent=4)
            logging.info(f"Wrote output JSON with commands to: {args.output_json}")
            return

    generate_bmx_cmds(main_clip_list,args.bmx)

    all_output_files = execute_commands(main_clip_list,"bmx_cmd")    
    # if bmx failed, try ffmpeg
    ffmpeg_clips = []
    for clip in main_clip_list:
        if not clip.processing_success:
            ffmpeg_clips.append(
                clip
            )

    generate_ffmpeg_copy_cmds(ffmpeg_clips,args.output,args.ffmpeg)

    execute_commands(ffmpeg_clips,"ffmpeg_copy_cmd")

    # if ffmpeg also failed, do a simple copy
    copy_clips = []
    seen_output_files = set()
    for clip in main_clip_list:
        #full copy only needs one output file
        if not clip.processing_success and clip.output_file not in seen_output_files:
            copy_clips.append(clip)
            seen_output_files.add(clip.output_file)

    if (len(copy_clips) > 0):
        copy_files_parallel(copy_clips,args.output)
        
    write_output_aaf(all_output_files)

if __name__ == '__main__':
    try:
        main()
    except otio.exceptions.OTIOError as err:
        logging.error("ERROR: " + str(err) + "\n")
        sys.exit(1)