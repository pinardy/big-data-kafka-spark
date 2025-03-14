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

The data labels and data dictionary can be found in the `/data` folder.
```
├──- labels.csv                Labels for driving trips safety
├──- data_dictionary.xlsx      Data dictionary to explain fields in dataset
```

The raw dataset source is in CSV format and can be found at [kaggle](https://www.kaggle.com/datasets/vancharmlab/grabai).

We have split the raw dataset into two parts:
1) [First part](https://drive.google.com/file/d/1uZFnSLJEk_KECungCZJBnf_M0wv2sUI-/view?usp=drive_link) to be served from a **FastAPI server** in **JSON** format
2) [Second part](https://drive.google.com/file/d/1EdybA11rurBooihyecUQUVHmDwN0_O1Q/view?usp=drive_link) to be stored in a **MinIO file server** in **CSV** format