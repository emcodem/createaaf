import os
import shutil
import subprocess
import json

def find_ffprobe():
    """
    Finds the ffprobe executable.
    First checks ../../../processors/ffmpeg/x64, then system PATH.
    Raises FileNotFoundError if not found.
    """
    # Check in custom folder
    custom_path = os.path.abspath("../../../processors/ffmpeg/x64/ffprobe")
    if os.name == "nt":
        custom_path += ".exe"
    
    if os.path.isfile(custom_path):
        return custom_path

    # Check in system PATH
    ffprobe_path = shutil.which("ffprobe")
    if ffprobe_path:
        return ffprobe_path

    raise FileNotFoundError(
        "ffprobe was not found in '../../../processors/ffmpeg/x64' or in the system PATH."
    )

def get_ffprobe_info(video_file):
    """
    Runs ffprobe on the given video file and returns the output.
    """
    cmd = ["ffprobe", '-of','json','-show_format','-show_streams', video_file]

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = p.communicate()
    if p.returncode != 0:
        raise subprocess.CalledProcessError(p.returncode, subprocess.list2cmdline(cmd), stderr)

    return json.loads(stdout.decode('utf8'))

    ffprobe_path = find_ffprobe()
    try:
        result = subprocess.run(
            [ffprobe_path, "-v", "error", "-show_format", "-show_streams", video_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"ffprobe failed: {e.stderr}")