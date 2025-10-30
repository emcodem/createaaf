import sys
import os
from inspect import currentframe, getframeinfo
from pathlib import Path

import types
import logging
import json

filename = getframeinfo(currentframe()).filename
parent = str(Path(filename).resolve().parent) # 
sys.path.append(parent)
parent = str(Path(filename).resolve().parent.parent) # parent.parent = go 1 up to find our own aaf2
sys.path.append(parent)
import aaf2
import aaf2.mxf
from aafhelpers import mxf_deep_search_by_key

__all__ = ["attachLUT"]

def ensureLUTFile():
    #just ensures the file exists, if not write with defaults
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_dir = os.path.dirname(script_dir) # goes 1 up
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

def autoLUT(lut_table: str, existing_mxf_file_path: str):
    # we choose lut only based on trc, not sure if we really need to consider other params, e.g. does HLG with 709 primaries exist?
    m = aaf2.mxf.MXFFile(existing_mxf_file_path)
    m.walker = types.MethodType(mxf_deep_search_by_key, m) #extend the MXFFile Class, we need "self" to work in mxf_deep_search_by_key
    trc = m.walker(search="TransferCharacteristic")
    if (trc == None):
        logging.debug("Autolut failed, no trc in " + existing_mxf_file_path)
    trc = aaf2.mxf.reverse_auid(trc).hex
    # prim = m.walker(search="ColorPrimaries")
    # eq = m.walker(search="CodingEquations")
    work_lut = next(
        (entry["lut"] for entry in lut_table if entry["trc"] == trc),
        None
    )
    if (work_lut == None):
        logging.debug("Autolut failed, no matching LUT found for trc" + trc)
    else:
        logging.debug("Autolut result: " + work_lut)
    return work_lut



def attachLUT(f: aaf2.file.AAFFile, existing_mxf_file_path: str, lut_select: str = "auto"):
    # search the aaf for the sourcepackage that matches the existing_mxf_files path
    # analyze the existing mxf to get out colors
    # find the color mapping in color_luts.json (transferchar) and adds a taggedvalue to the sourcepackage
    # the taggedvalue contains _COLOR_INPUT_TRANSFORMATION, an xml that is avid specific, it points to cube files that come with avid mcp
    
    #ensure color luts file exists
    if lut_select == None: #no userinput no work
        return
    
    #cares about LUT mapping file
    lut_file = ensureLUTFile()
    lut_table = {}  
    with open(lut_file, "r", encoding="utf-8") as _tmp:
        lut_table = json.load(_tmp)
    
    all_packages = f.content.mobs
    
    video_sources = []
    for pkg in all_packages:
        
        if isinstance(pkg, aaf2.mobs.SourceMob):
            if isinstance(pkg.descriptor, aaf2.essence.CDCIDescriptor):
                # this seems to be the structure of a "non ama" mxf
                video_sources.append(pkg)
                
                #TODO Filter by mxf path match networklocator_path

            if isinstance(pkg.descriptor, aaf2.essence.MultipleDescriptor):
                networklocator_path = next(iter(pkg.descriptor.locator[0].property_entries.values())).value
                video_sources.append(pkg)
                #TODO Filter by mxf path match networklocator_path

                # this seems to be the structure of an ama mxf
                # video_sources = [
                #                     obj for obj in pkg.descriptor["FileDescriptors"].objects.values()
                #                     if isinstance(obj, aaf2.essence.CDCIDescriptor)
                #                 ]   


    #select lut from userparam from color_luts.json
    work_lut = None
    if lut_select == "auto": 
        try:
            work_lut = autoLUT(lut_table, existing_mxf_file_path)
        except:
            logging.debug("Autolut failed, no trc in " + existing_mxf_file_path)
            pass
    else:
        work_lut = next(
            (entry["lut"] for entry in lut_table if entry["name"] == lut_select), #untested mode
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