#core modules
import sys
import sys 
import os
import json
import types
import struct
import xml.etree.ElementTree as ET
# Install packages in venv (after moving the venv path):
# python -m venv --upgrade C:\FFAStrans-Public-1.4.2\avid_tools\0.7\createaaf\venv
# C:\FFAStrans-Public-1.4.2\avid_tools\0.7\createaaf\venv\Scripts\activate
# get-command pip (must be in the venv)
# python -m pip install hachoir

#addon modules (portable)
from inspect import currentframe, getframeinfo
from pathlib import Path
filename = getframeinfo(currentframe()).filename
parent = str(Path(filename).resolve().parent)
sys.path.append(parent)
import aaf2 # make sure you use our private aaf2 version
import aaf2.mxf
from aaf_helpers.aafhelpers import mxf_deep_search_by_key
from aaf_helpers.avid_lut import translateSonyXmlValue

# Check if env var ffas_py_args is set, if yes, push to sys.argv
if 'ffas_py_args' in os.environ:
    args = os.environ['ffas_py_args'].split()
    sys.argv.extend(args)

result = ""

def extract_xml_from_sony_mp4(file_path, target_uuid=None):
    """
    Parses the colors from Sony embedded xml in mp4 files.
    Looks for "meta" boxes containing XML data.
    """
    with open(file_path, "rb") as f:
        while True:
            header = f.read(8)
            if len(header) < 8:
                break  # End of file
            
            size32, box_type = struct.unpack(">I4s", header)
            box_type = box_type.decode('utf-8', errors='ignore')
            if box_type in ("ftyp"):
                # Read next 4 bytes to check for "XAVC"
                brand = f.read(4)
                if brand != b"XAVC":
                    return None
                # Seek back 4 bytes
                f.seek(-4, 1)

            # Determine actual size
            if size32 == 1:
                # extended size: next 8 bytes
                size_bytes = f.read(8)
                if len(size_bytes) < 8:
                    break
                size = struct.unpack(">Q", size_bytes)[0]
                header_size = 16  # 4 + 4 + 8
            else:
                size = size32
                header_size = 8

            if size < header_size:
                break  # corrupted atom

            payload_size = size - header_size
            if payload_size > 1048_576 * 10:  # 10 MB limit for sanity:  skip very large boxes
                f.seek(payload_size, 1)
                continue

            if box_type in ("meta"):
                # meta box has version/flags at start, then handler
                payload = f.read(payload_size)
                
                # Read version + flags (4 bytes)
                if len(payload) >= 4:
                    version_flags = struct.unpack(">I", payload[:4])[0]
                    
                if b"<?xml" in payload:
                    start = payload.find(b"<?xml")
                    xml_bytes = payload[start:-1]

                    try:
                        xml_text = xml_bytes.decode("utf-8", errors="ignore")
                        # sanitize control characters
                        xml_text = "".join(c for c in xml_text if c >= " " or c in "\t\n\r")
                        root = ET.fromstring(xml_text)
                        
                        # Parse Sony color metadata from AcquisitionRecord/Group[@name="CameraUnitMetadataSet"]
                        result = {}
                        result["xml"] = root
                        group = root.find('.//*[@name="CameraUnitMetadataSet"]')
                        
                        if group is not None:
                            
                            for item in group.findall('./*[@name]'):
                                name = item.get('name')
                                value = item.get('value')
                                if name in ('CaptureGammaEquation'):
                                    _translated = translateSonyXmlValue("trc_" + value)
                                    if(_translated):
                                        result["TransferCharacteristic"] = _translated
                                if name in ('CaptureColorPrimaries'):
                                    _translated = translateSonyXmlValue("prim_" + value)
                                    if(_translated):
                                        result["ColorPrimaries"] = _translated
                                if name in ('CodingEquations'):
                                    _translated = translateSonyXmlValue("ceq_" + value)
                                    if(_translated):
                                        result["CodingEquations"] = _translated
                            return result
                        
                        return None
                    except ET.ParseError as e:
                        print(f"XML Parse Error: {e}")
                        return None

            else:
                # skip payload
                f.seek(payload_size, 1)
    return None


def main():
    
    import os
    global result
    global ffas_py_args
    result = ""
    # Check if env var ffas_py_args is set, if yes, push to sys.argv
    ffas_py_args = ffas_py_args.decode('utf-8') if 'ffas_py_args' in globals() else False
    if ffas_py_args:
        sys.argv.extend(ffas_py_args)
    if len(sys.argv) < 2:
        print("Usage: analyze_mxf_colors.py <file_path>")
        sys.exit(1)
    
    try:
        # check if extension is mp4, if yes throw exception
        if (sys.argv[1].lower().endswith(".mp4")):
            raise Exception("This is an mp4 file.")
        m = aaf2.mxf.MXFFile(sys.argv[1])

        m.walker = types.MethodType(mxf_deep_search_by_key, m) #extend the MXFFile Class, we need "self" to work in mxf_deep_search_by_key
        
        trc = m.walker(search="TransferCharacteristic")
        prim = m.walker(search="ColorPrimaries")
        eq = m.walker(search="CodingEquations")
        

        if (trc):
            _toprint = " --transfer-ch urn:smpte:ul:" + aaf2.mxf.reverse_auid(trc).hex
            print(_toprint, end=" ")
            result += _toprint
        if (prim):
            _toprint = " --color-prim urn:smpte:ul:" + aaf2.mxf.reverse_auid(prim).hex
            print(_toprint, end=" ")
            result += _toprint
        if (eq):
            _toprint = " --coding-eq urn:smpte:ul:" + aaf2.mxf.reverse_auid(eq).hex
            print(_toprint, end=" ")
            result += _toprint
        return result
    except:
        try:
            colors = extract_xml_from_sony_mp4(str(sys.argv[1]))
            if colors:
                if "TransferCharacteristic" in colors:
                    _toprint = f" --transfer-ch urn:smpte:ul:{colors['TransferCharacteristic']}"
                    print(_toprint, end=" ")
                    result += _toprint
                if "ColorPrimaries" in colors:
                    _toprint = f" --color-prim urn:smpte:ul:{colors['ColorPrimaries']}"
                    print(_toprint, end=" ")
                    result += _toprint
                if "CodingEquations" in colors:
                    _toprint = f" --coding-eq urn:smpte:ul:{colors['CodingEquations']}"
                    print(_toprint, end=" ")
                    result += _toprint
            return result
        except:
            pass
        
        print("") # no colors found or newline


if __name__ == '__main__':
    
    try:
        result = main()
        if (result is None):
            result = ""
    except Exception as e:
        result = e
        print(f"Error: {e}")

