# Big Data Engineering Practice Module

## Folder Structure

```
├──- docker-compose.yml              Docker compose file to start up all application containers
├──- docker-compose-storage.yml      Docker compose file to start up all storage containers
🗂️── data                            Store data to be used for application (Would be made avaiable upon startup of MinIO)
🗂️── consumer                        Consumer application code
🗂️── producer                        Producer application code
```
## Getting Started

### Docker
Ensure that docker is already installed.

Start all services with:
```sh
docker-compose -f docker-compose-kafka.yml -f docker-compose-storage.yml -f docker-compose.yml up -d
```

You can view the started containers on Docker Desktop.

Stop all services with:
```sh
docker-compose -f docker-compose-kafka.yml -f docker-compose-storage.yml -f docker-compose.yml down
```

### Python Virtual Environment

1) Create your own virtual environment:
```sh
python3 -m venv myenv
```

2) Activate the virtual environment:
```sh
source myenv/bin/activate
```

3) Install the python packages in the virtual environment:
```sh
pip install -r requirements.txt
```

### Services

| Service Name | Description                      | URL (UI)              | Notes                                                             |
|--------------|----------------------------------|-----------------------|-------------------------------------------------------------------|
| Postgres     | Database storage for application | http://localhost:8080 | Login details found in docker-compose.yml                         |
| MinIO        | Object storage (S3 compatible)   | http://localhost:9090 | Webpage access to view and configurate MinIO                      |
| MinIO Client | MinIO API calls                  | http://localhost:9000 | No webpage access but used for application to access the content. |

## Data

The data labels and data dictionary can be found in the `/data` folder.
```
├──- labels.csv                Labels for driving trips safety
├──- data_dictionary.xlsx      Data dictionary to explain fields in dataset
  ├──- raw                     Contains Raw data used in the application, manual extraction from kaggle required.

```

The raw dataset source is in CSV format and can be found at [kaggle](https://www.kaggle.com/datasets/vancharmlab/grabai).

We have split the raw dataset into two parts:
1) [First part](https://drive.google.com/file/d/1uZFnSLJEk_KECungCZJBnf_M0wv2sUI-/view?usp=drive_link) to be served from a **FastAPI server** in **JSON** format
2) [Second part](https://drive.google.com/file/d/1EdybA11rurBooihyecUQUVHmDwN0_O1Q/view?usp=drive_link) to be stored in a **MinIO file server** in **CSV** format