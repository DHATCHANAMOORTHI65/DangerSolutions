from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {"message": "Danger Solutions Backend Running Successfully!"}

@app.get("/status")
def status():
    return {"status": "OK", "version": "1.0"}
