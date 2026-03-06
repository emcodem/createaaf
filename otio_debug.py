
import sys
import os
from aaftimelineparser import CutClipList
sys.path.append(os.path.join(os.path.dirname(__file__), "venv/Lib/site-packages/")) #for e.g. OTIO

import opentimelineio as otio
from opentimelineio.media_linker import MediaLinker
from opentimelineio.schema import ExternalReference
import xml.etree.ElementTree as ET

input_file = sys.argv[1]

in_adapter = otio.adapters.from_filepath(input_file).name
result_tl = otio.adapters.read_from_file(input_file, in_adapter)

for track in result_tl.tracks:
    if track.kind != "Video":
        continue

    for item in track:
        if not isinstance(item, otio.schema.Clip):
            continue
        sr = item.source_range
        print (sr.start_time.value)
        print (sr.duration.value)