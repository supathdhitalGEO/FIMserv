import os
import csv
import pandas as pd
import subprocess


def setup_directories():
    parent_dir = os.getcwd()
    code_dir = os.path.join(parent_dir, "code", "inunation-mapping")
    data_dir = os.path.join(parent_dir, "data", "inputs")
    output_dir = os.path.join(parent_dir, "output")

    # Create the directories if not exist
    os.makedirs(code_dir, exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    return code_dir, data_dir, output_dir


def clone_repository(code_dir):
    repo_path = os.path.join(code_dir)

    # Check if repository folder exists and has files in it
    if os.path.exists(repo_path) and os.listdir(repo_path):
        print(
            f"Repository already exists at {repo_path} and contains files. Skipping clone."
        )
    else:
        # Clone the repository if it doesn't exist or is empty
        repo_url = "https://github.com/NOAA-OWP/inundation-mapping.git"
        subprocess.run(["git", "clone", repo_url, repo_path], check=True)
        print(f"Repository cloned into: {repo_path}")


def download_data(huc_number, base_dir):
    output_dir = os.path.join(base_dir, f"flood_{huc_number}", str(huc_number))

    # Create the directory structure if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # For the CIROH
    cmd = f"aws s3 sync s3://ciroh-owp-hand-fim/hand_fim_4_5_2_11/{huc_number}/ {output_dir} --no-sign-request "

    # Run the AWS CLI command
    os.system(cmd)
    print(f"Data for HUC {huc_number}")

    # Now copying the hydrotable.csv to the outside of directory as fim_inputs.csv
    hydrotable_path = os.path.join(output_dir, "branch_ids.csv")
    fim_inputs_path = os.path.join(base_dir, f"flood_{huc_number}", "fim_inputs.csv")

    # Read the first row from branch_ids.csv
    with open(hydrotable_path, "r") as infile:
        reader = csv.reader(infile)
        first_row = next(reader)  # Get the first row

    # Write the first row to fim_inputs.csv
    with open(fim_inputs_path, "w", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(first_row)

    print(
        f"Copied the first row of {hydrotable_path} to {fim_inputs_path} as fim_inputs.csv."
    )


def uniqueFID(hydrotable, fid_dir, stream_order=None):
    hydrotable_df = pd.read_csv(hydrotable)
    if stream_order:
        stream_order = [str(order) for order in stream_order]
        hydrotable_df["order_"] = hydrotable_df["order_"].astype(str)
        hydrotable_df_filtered = hydrotable_df[
            hydrotable_df["order_"].isin(stream_order)
        ]
        unique_FIDs = hydrotable_df_filtered["feature_id"].drop_duplicates()
    else:
        unique_FIDs = hydrotable_df["feature_id"].drop_duplicates()
    unique_FIDs_df = pd.DataFrame(unique_FIDs, columns=["feature_id"])
    unique_FIDs_df.to_csv(fid_dir, index=False)
    print(f"Unique feature IDs saved to {fid_dir}.")


# IN the replacement of Docker, the environement variables are set in the .env file
def EnvFile(code_dir):
    env_dir = os.path.join(code_dir, ".env")
    env_content = """
    inputsDir=inputs
    outputsDir=output
    """
    os.makedirs(os.path.dirname(env_dir), exist_ok=True)

    with open(env_dir, "w") as f:
        f.write(env_content)


def DownloadHUC8(huc, stream_order=None):
    code_dir, data_dir, output_dir = setup_directories()
    clone_repository(code_dir)

    # if huc is not str:
    huc = str(huc)
    download_data(huc, output_dir)
    EnvFile(code_dir)
    HUC_dir = os.path.join(output_dir, f"flood_{huc}")
    hydrotable_dir = os.path.join(HUC_dir, str(huc), "hydrotable.csv")
    featureID_dir = os.path.join(HUC_dir, f"feature_IDs.csv")
    if stream_order is None:
        uniqueFID(hydrotable_dir, featureID_dir)
    else:
        uniqueFID(hydrotable_dir, featureID_dir, stream_order)
