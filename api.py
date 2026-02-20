from fastapi import FastAPI

app = FastAPI()

items = []

@app.post("/test_post")
def post_item(item: str):
    items.append(item)
    return items

@app.get("/test_get")
def get_items():
    return items

@app.get("/")
def default_return():
    return "Sup"