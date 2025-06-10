from fastapi import FastAPI, HTTPException
from typing import Dict, Any
import json

from scrapers.scraper_powr_connect import scrape_powr_connect
from scrapers.scraper_voltaneo import scrape_voltaneo
from scrapers.scraper_eklor import scrape_eklor

app = FastAPI()

@app.post("/scrape-powr-connect")
async def scrape_powr_connect_endpoint(payload: Dict[str, Any]):
    try:
        df = await scrape_powr_connect(payload)
        return json.loads(df.to_json(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")

@app.post("/scrape-voltaneo")
async def scrape_voltaneo_endpoint(payload: Dict[str, Any]):
    try:
        df = await scrape_voltaneo(payload)
        return json.loads(df.to_json(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")

@app.post("/scrape-eklor")
async def scrape_eklor_endpoint(payload: Dict[str, Any]):
    try:
        df = await scrape_eklor(payload)
        return json.loads(df.to_json(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")