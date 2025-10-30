import os
import shutil
import subprocess

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