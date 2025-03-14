# Big Data Engineering Practice Module

## Folder Structure

```
├──- docker-compose.yml      Docker compose file to start up all application containers
🗂️── data                    Store data to be used for application
🗂️── consumer                Consumer application code
🗂️── producer                Producer application code
```
## Getting Started

Ensure that docker is already installed.

Start all services with:
```sh
docker-compose up -d
```

You can view the started containers on Docker Desktop.

Stop all services with:
```sh
docker-compose down
```

## Data

The data can be found in the `/data` folder.
```
├──- labels.csv                Labels for driving trips safety
├──- data_dictionary.xlsx      Data dictionary to explain fields in dataset
```

The dataset source can be found at [kaggle](https://www.kaggle.com/datasets/vancharmlab/grabai).

We have split the dataset into two parts:
1) First part to be served from a **FastAPI server** in **JSON** format
2) Second part to be stored in a **MinIO file server** in **CSV** format

**TODO**: Upload the data for the two parts