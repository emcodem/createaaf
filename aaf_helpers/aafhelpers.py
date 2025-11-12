#core python modules
import sys
from inspect import currentframe, getframeinfo
from pathlib import Path

#addon modules (portable)
filename = getframeinfo(currentframe()).filename
parent = str(Path(filename).resolve().parent.parent) # parent.parent = go 1 up to find our own aaf2
sys.path.append(parent)

import aaf2
import aaf2.mxf

def mxf_deep_search_by_key(self, obj=None, search="TransferCharacteristic"):
    for uuid, obj in self.objects.items():
        if isinstance(obj.data, dict):
            if (search in obj.data):
                return obj.data[search]



                
    # if obj is None:
    #     obj = self.preface
    # for key, value in sorted(obj.data.items()):
    #     if isinstance(value, aaf2.mxf.MXFRef):
    #         c = self.objects.get(value, None)
    #         if c:
    #             found = self.walker(c, search)
    #             if found is not None:
    #                 return found

    #     elif isinstance(value, aaf2.mxf.MXFRefArray):
    #         for item in value:
    #             c = self.objects.get(item, None)
    #             if c:
    #                 found = self.walker(c, search)
    #                 if found is not None:
    #                     return found
    #     else:
    #         if (key == search):
    #             return value
            