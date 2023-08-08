from fastapi import FastAPI
from routers import accounts, messages

app = FastAPI(
    title="guestbook API",
    version="0.1.0",
    description='Messages, suggestions and comments management'
)

app.include_router(accounts.router)
app.include_router(messages.router)
