#core modules
import sys
import sys 
import glob, os
import json
import types

#addon modules (portable)
from inspect import currentframe, getframeinfo
from pathlib import Path
filename = getframeinfo(currentframe()).filename
parent = str(Path(filename).resolve().parent)
sys.path.append(parent)
import aaf2
import aaf2.mxf

from aaf_helpers.aafhelpers import mxf_deep_search_by_key
try:
    m = aaf2.mxf.MXFFile(sys.argv[1])
    m.walker = types.MethodType(mxf_deep_search_by_key, m) #extend the MXFFile Class, we need "self" to work in mxf_deep_search_by_key
    trc = m.walker(search="TransferCharacteristic")
    prim = m.walker(search="ColorPrimaries")
    eq = m.walker(search="CodingEquations")

    if (trc):
        print(" --transfer-ch urn:smpte:ul:" + aaf2.mxf.reverse_auid(trc).hex, end=" ")
    if (prim):
        print(" --color-prim urn:smpte:ul:" + aaf2.mxf.reverse_auid(prim).hex, end=" ")
    if (eq):
        print(" --coding-eq urn:smpte:ul:" + aaf2.mxf.reverse_auid(eq).hex, end=" ")

except:
    print("") # no colors found
