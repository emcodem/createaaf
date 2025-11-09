import sys 
from inspect import currentframe, getframeinfo
from pathlib import Path
from urllib.parse import urlparse,unquote
import traceback
import logging

#add module paths to sys path, otherwise portable python cannot resolve it
filename = getframeinfo(currentframe()).filename
parent = str(Path(filename).resolve().parent)
sys.path.append(parent)
import aaf2
import aaf2.mxf

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