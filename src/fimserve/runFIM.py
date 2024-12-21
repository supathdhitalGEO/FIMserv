import os
import sys
import glob
import subprocess
from dotenv import load_dotenv

from .datadownload import setup_directories


def runfim(code_dir, output_dir, HUC_code, data_dir):
    original_dir = os.getcwd()
    try:
        tools_path = os.path.join(code_dir, "tools")
        src_path = os.path.join(code_dir, "src")
        os.chdir(tools_path)
        dotenv_path = os.path.join(code_dir, ".env")
        load_dotenv(dotenv_path)
        sys.path.append(src_path)
        sys.path.append(code_dir)
        HUC_code = str(HUC_code)
        HUC_dir = os.path.join(output_dir, f"flood_{HUC_code}")

        csv_path = data_dir

        # Get the inundation file path
        discharge_basename = os.path.basename(data_dir).split("_")[0]
        inundation_file = os.path.join(
            HUC_dir, f"{HUC_code}_inundation/{discharge_basename}_inundation.tif"
        )

        Command = [
            sys.executable,
            "inundate_mosaic_wrapper.py",
            "-y",
            HUC_dir,
            "-u",
            HUC_code,
            "-f",
            csv_path,
            "-i",
            inundation_file,
        ]
        env = os.environ.copy()
        env["PYTHONPATH"] = f"{src_path}{os.pathsep}{code_dir}"

        result = subprocess.run(
            Command,
            cwd=tools_path,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Print the output and error (if any)
        print(result.stdout.decode())
        if result.stderr:
            print(result.stderr.decode())
        # Check if the command was successful
        if result.returncode == 0:
            print(f"Inundation mapping for {HUC_code} completed successfully.")
        else:
            print(f"Failed to complete inundation mapping for {HUC_code}.")

    finally:
        os.chdir(original_dir)


def runOWPHANDFIM(huc):
    code_dir, data_dir, output_dir = setup_directories()
    discharge = os.path.join(data_dir, f"*_{huc}.csv")
    files = glob.glob(discharge)
    for file in files:
        runfim(code_dir, output_dir, huc, file)
