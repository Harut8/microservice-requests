from fastapi import FastAPI

from custom_requests import create_get_request

app = FastAPI()


@app.get("/")
async def root():
    await create_get_request("users/")
    return {"message": "Hello World"}

from uvicorn import run
run(app, host="0.0.0.0", port=8000)
