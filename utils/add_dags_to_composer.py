# import required libraries
import argparse
import glob
import os
import tempfile
from shutil import copytree, ignore_patterns
from google.cloud import storage

def _create_file_list(directory: str, name_replacement: str) -> tuple[str, list[str]]:
    """Create a temporary directory and copy files from the specified directory while ignoring certain patterns."""
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist. Skipping upload.")
        return "", [] # Return empty values if directory doesn't exist so that script does not fail
    
    temp_dir = tempfile.mkdtemp() # Create a temporary directory
    files_to_ignore = ignore_patterns("__init__.py", "*_test.py") # Define patterns to ignore
    copytree(directory, f"{temp_dir}/", ignore=files_to_ignore, dirs_exist_ok=True) # Copy files to temporary directory while ignoring specified patterns

    # ensure only files are returned
    files = [f for f in glob.glob(f"{temp_dir}/**", recursive=True) if os.path.isfile(f)]
    return temp_dir, files

def upload_to_composer(directory: str, bucket_name:str, name_replacement: str) -> None:
    """Upload DAGs and Data files to Google Cloud Storage bucket."""
    temp_dir, files = _create_file_list(directory, name_replacement)
    if not files:
        print("No files to upload. Exiting.")
        return
    storage_client = storage.Client() # Initialize Google Cloud Storage client
    bucket = storage_client.bucket(bucket_name) # Get the specified bucket
    for file in files:
        file_gcs_path = file.replace(f"{temp_dir}/", name_replacement) # Create the GCS path by replacing the temporary directory path with the name replacement
        try:
            blob = bucket.blob(file_gcs_path) # Create a blob object for the file
            blob.upload_from_filename(file) # Upload the file to GCS
            print(f"Uploaded {file} to gs://{bucket_name}/{file_gcs_path}")
        except IsADirectoryError:
            print(f"Skipping directory {file}, only files are uploaded.")
        except FileNotFoundError:
            print(f"File {file} not found. Ensure directory structure is correct.")
            raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload DAGs and Data files to Cloud Composer.")
    parser.add_argument("--dags_directory", help="Path to DAGs directory.")
    parser.add_argument("--dags_bucket", help="GCS bucket name for DAGs.")
    parser.add_argument("--data_directory", help="Path to data directory.")
    args = parser.parse_args()
    print(args.dags_directory, args.dags_bucket, args.data_directory)
    
    if args.dags_directory and os.path.exists(args.dags_directory):
        upload_to_composer(args.dags_directory, args.dags_bucket, "dags/")
    else:
        print(f"DAGs directory {args.dags_directory} does not exist. Skipping DAGs upload.")
    
    if args.data_directory and os.path.exists(args.data_directory):
        upload_to_composer(args.data_directory, args.dags_bucket, "data/")
    else:
        print(f"Data directory {args.data_directory} does not exist. Skipping data upload.")