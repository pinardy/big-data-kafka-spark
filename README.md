# Big Data Engineering Practice Module

## Folder Structure

```
├──- docker-compose-kafka.yml        Docker compose file to start up all Kafka services containers
├──- docker-compose.yml              Docker compose file to start up all application containers
├──- docker-compose-storage.yml      Docker compose file to start up all storage containers
🗂️── backend                         FastAPI server to serve data to frontend
🗂️── batch-processing                Batch processing using PySpark
🗂️── consumer                        Consumer application code
🗂️── data                            Store data to be used for application (Would be made avaiable upon startup of MinIO)
🗂️── frontend                        Frontend for visualizing insights on telematics data and ride safety
🗂️── producer                        Producer application code
```
## Getting Started

### Docker
Ensure that docker is already installed. 

You will need to build the following images in their respective folders:

```sh
# consumer image (run in consumer folder)
docker build -t consumer .

# producer image (run in producer folder)
docker build -t producer .

# batch-processing image (run in batch-processing folder)
docker build -t batch-processing .

# frontend image (run in frontend folder)
docker build -t frontend .

# backend image (run in backend folder)
docker build -t backend .
```

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

| Service Name                | Description                                                    | URL                   | Notes                                                             |
|-----------------------------|----------------------------------------------------------------|-----------------------|-------------------------------------------------------------------|
| Postgres                    | Database storage for application                               | http://localhost:8080 | Login details found in docker-compose-storage.yml                 |
| MinIO                       | Object storage (S3 compatible)                                 | http://localhost:9090 | Webpage access to view and configurate MinIO                      |
| MinIO Client                | MinIO API calls                                                | http://localhost:9000 | No webpage access but used for application to access the content. |
| Kafdrop                     | Kafka UI for checking topics & messages                        | http://localhost:9001 |                                                                   |
| Backend                     | FastAPI backend to serve API endpoints                         | http://localhost:8000 | For displaying telematics data on frontend                        |
| Frontend                    | React frontend                                                 | http://localhost:3000 | Frontend to display telematics data                               |  
| Batch Ingestion of Raw Data | Service to carry out ingestion of raw data from files in minio | http://localhost:8081 | Called when in need to ingest raw files into the system           |  
| Telematics consolidation    | Service to carry out telematics consolidation of rides         | http://localhost:8082 | Called periodically to consolidate ride information captured      |  
  
## Data

The data labels and data dictionary can be found in the `/data` folder.
```
├──- labels.csv                Labels for driving trips safety
├──- data_dictionary.xlsx      Data dictionary to explain fields in dataset
  ├──- raw                     Contains raw data used in the application. Manual extraction from the raw dataset below required
```

The raw dataset source is in CSV format and can be found at [kaggle](https://www.kaggle.com/datasets/vancharmlab/grabai).

We have split the raw dataset into two parts:

1. [First part](https://drive.google.com/file/d/1uZFnSLJEk_KECungCZJBnf_M0wv2sUI-/view?usp=drive_link) to be served from a **FastAPI server** in **JSON** format
2. [Second part](https://drive.google.com/file/d/1EdybA11rurBooihyecUQUVHmDwN0_O1Q/view?usp=drive_link) to be stored in a **MinIO file server** in **CSV** format

Download the first part and place it in within the `producer` folder.