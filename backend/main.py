from fastapi import FastAPI

app = FastAPI()

# TODO: create endpoints to expose telematics data from postgres

@app.get("/")
async def root():
    return {"message": "Hello World"}