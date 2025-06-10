import asyncio
import pandas as pd
from playwright.async_api import async_playwright
import json
from datetime import datetime
import re

async def accept_cookies_eklor(page):
    """
    Accepte la bannière de cookies sur le site Eklor si elle est présente.
    """
    try:
        await page.click('text="OK pour moi"', timeout=3000)
    except:
        pass

async def login_eklor(page, email, password):
    """
    Se connecte au site Eklor avec les identifiants fournis.
    """
    await page.goto("https://eklor.shop/login")
    await page.wait_for_selector('input[type="email"]')
    await page.fill('input[type="email"]', email)
    await page.fill('input[type="password"]', password)
    await accept_cookies_eklor(page)
    await page.click('button[type="submit"]')
    await page.wait_for_url("https://eklor.shop/", timeout=10000)

async def scrape_product_eklor(page, item):
    """
    Scrape les informations détaillées d'un produit Eklor à partir de son URL.
    """
    url = item["url"]
    errors = []
    data = {}

    try:
        await page.goto(url)
        await page.wait_for_load_state("domcontentloaded")
        await accept_cookies_eklor(page)
    except Exception as e:
        return {
            **item,
            **{f: "N/A" for f in ["name", "reference", "price_per_unit", "description", "technical_ref"]},
            "is_ok": 0,
            "error": f"page.goto failed: {str(e)}"
        }

    try:
        name = await page.text_content('h1.mb-4.text-2xl.font-medium')
        data["name"] = name.strip() if name else "N/A"
        if not name:
            errors.append("missing name")
    except Exception as e:
        data["name"] = "N/A"
        errors.append(f"name error: {str(e)}")

    try:
        price = await page.text_content('span.text-3xl.font-semibold')
        data["price_per_unit"] = price.strip() if price else "N/A"
        if not price:
            errors.append("missing price")
    except Exception as e:
        data["price_per_unit"] = "N/A"
        errors.append(f"price error: {str(e)}")

    try:
        stock = await page.text_content('button.Stock-label.Stock-label')
        data["stock"] = stock.strip() if stock else "N/A"
        if not stock:
            errors.append("missing stock")
    except Exception as e:
        data["stock"] = "N/A"
        errors.append(f"stock error: {str(e)}")

    try:
        summary = await page.text_content('p.mb-6.text-base.font-normal')
        data["description"] = summary.strip() if summary else "N/A"
        if not summary:
            errors.append("missing description")
    except Exception as e:
        data["description"] = "N/A"
        errors.append(f"description error: {str(e)}")

    try:
        await page.wait_for_selector('li.bullet-list', timeout=3000)
        tech_elements = await page.locator('li.bullet-list').all_text_contents()
        data["technical_ref"] = tech_elements if tech_elements else "N/A"
        if not tech_elements:
            errors.append("missing technical_ref")
    except Exception as e:
        data["technical_ref"] = "N/A"
        errors.append(f"technical_ref error: {str(e)}")

    return {
        **item,
        **data,
        "is_ok": 0 if errors else 1,
        "error": "; ".join(errors) if errors else None
    }

def clean_output_eklor(output_df):
    """
    Nettoie et enrichit le DataFrame final des résultats du scraping Eklor.
    """
    output_df['price_per_unit'] = output_df['price_per_unit'].astype(str).apply(
        lambda x: re.findall(r'[\d,]+', x)[0] if re.findall(r'[\d,]+', x) else "N/A"
    )
    output_df['price_per_unit'] = output_df['price_per_unit'].str.replace(',', '.').astype(float, errors='ignore')
    output_df['is_available'] = output_df['stock'].apply(
        lambda x: 1 if isinstance(x, str) and 'produits en stock' in x.lower() else 0
    )
    output_df['created_at'] = datetime.now()

    output_df['unit_1'] = "À l'unité"
    output_df['price_per_unit_1'] = output_df['price_per_unit']
    output_df['unit_2'] = "N/A"
    output_df['price_per_unit_2'] = "N/A"
    output_df['unit_3'] = "N/A"
    output_df['price_per_unit_3'] = "N/A"

    columns_order = [
        "product_category",
        "manufacturer",
        "manufacturer_id",
        "supplier",
        "url",
        "name",
        "description",
        "technical_ref",
        "unit_1",
        "price_per_unit_1",
        "unit_2",
        "price_per_unit_2",
        "unit_3",
        "price_per_unit_3",
        "is_available",
        "stock",
        "is_ok",
        "error",
        "created_at"
    ]
    output_df = output_df.reindex(columns=columns_order)

    return output_df

async def scrape_eklor(payload, headless=True):
    """
    Lance la session Playwright et exécute le scraping pour chaque produit Eklor.
    """
    credentials = payload["credentials"]
    data = payload["data"]
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=['--disable-blink-features=AutomationControlled']
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800}
        )
        page = await context.new_page()

        try:
            await login_eklor(page, credentials["username"], credentials["password"])
        except:
            return [{"error": "login_failed"}]

        for item in data:
            try:
                result = await scrape_product_eklor(page, item)
            except Exception as e:
                result = {**item, "error": str(e), "status": "failed"}
            results.append(result)

        await browser.close()

    output = pd.DataFrame(results)
    output = clean_output_eklor(output)

    return output

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        payload_file = sys.argv[1]
    else:
        payload_file = "payload_e.json"
    with open(payload_file, "r", encoding="utf-8") as f:
        payload = json.load(f)
    output = asyncio.run(scrape_eklor(payload, headless=False))
    print(output)