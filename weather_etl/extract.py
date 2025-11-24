import os
import zipfile
import kaggle

def extract_data(**context):
    raw_path = os.path.join(os.path.dirname(__file__), "data/raw")
    os.makedirs(raw_path, exist_ok=True)

    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(
        'rteja/weatherhistory',
        path=raw_path,
        unzip=False
    )

    zip_file = os.path.join(raw_path, "weatherhistory.zip")
    if not os.path.exists(zip_file):
        raise FileNotFoundError(f"Zip file not found: {zip_file}")
    with zipfile.ZipFile(zip_file, 'r') as z:
        z.extractall(raw_path)


    csv_path = os.path.join(raw_path, "weatherHistory.csv")
    context['ti'].xcom_push(key="raw_csv_path", value=csv_path)

    print(f"Extraction complete. File saved to: {csv_path}")
