import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from datetime import datetime
import re
import json

async def accept_cookies_powr_connect(page):
    """
    Accepte la bannière de cookies sur le site Powr Connect si elle est présente.
    """
    try:
        await page.wait_for_selector('div[class*="axeptio_widget_wrapper"]', timeout=3000)
        await page.locator('button:has-text("OK pour moi")').click()
        await asyncio.sleep(0.5)
    except:
        pass

async def login_powr_connect(page, email, password):
    """
    Se connecte au site Powr Connect avec les identifiants fournis.
    """
    await page.goto("https://powr-connect.shop/connexion")
    await accept_cookies_powr_connect(page)
    await page.fill('input[name="username"]', email)
    await page.fill('input[name="password"]', password)
    try:
        await page.check('input[name="stayConnected"]', force=True)
    except:
        pass
    await page.evaluate("""
        () => {
            const form = document.querySelector('form[action*="/connexion"]');
            if (form) form.submit();
        }
    """)
    await page.wait_for_url(lambda url: not url.endswith("/connexion"), timeout=10000)

async def scrape_product_powr_connect(page, item):
    """
    Scrape les informations détaillées d'un produit Powr Connect à partir de son URL.
    """
    url = item["url"]
    errors = []
    data = {}

    try:
        await page.goto(url)
        await page.wait_for_load_state("domcontentloaded")
        await accept_cookies_powr_connect(page)
    except Exception as e:
        return {
            **item,
            **{f: "N/A" for f in ["name", "description", "price_per_unit", "stock", "technical_ref"]},
            "is_ok": 0,
            "error": f"page.goto failed: {str(e)}"
        }

    try:
        name = await page.text_content('h1.text-2xl.font-semibold.tracking-tight')
        data["name"] = name.strip() if name else "N/A"
        if not name:
            errors.append("missing name")
    except Exception as e:
        data["name"] = "N/A"
        errors.append(f"name error: {str(e)}")

    try:
        desc = await page.text_content('p.mt-4')
        data["description"] = desc.strip() if desc else "N/A"
        if not desc:
            errors.append("missing description")
    except Exception as e:
        data["description"] = "N/A"
        errors.append(f"description error: {str(e)}")

    try:
        price = await page.text_content('p.text-2xl.font-semibold.leading-none')
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
        await page.wait_for_selector('ul.bulleted-list li', timeout=3000)
        tech = await page.locator('ul.bulleted-list li').all_text_contents()
        data["technical_ref"] = tech if tech else "N/A"
        if not tech:
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

def clean_output_powr_connect(output_df):
    """
    Nettoie et enrichit le DataFrame final des résultats du scraping Powr Connect.
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

async def scrape_powr_connect(payload, headless=True):
    """
    Lance la session Playwright et exécute le scraping pour chaque produit Powr Connect.
    """
    credentials = payload["credentials"]
    data = payload["data"]
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            await login_powr_connect(page, credentials["username"], credentials["password"])
        except:
            return [{"error": "login_failed"}]

        for item in data:
            try:
                result = await scrape_product_powr_connect(page, item)
            except Exception as e:
                result = {**item, "error": str(e), "status": "failed"}
            results.append(result)

        await browser.close()

    output = pd.DataFrame(results)
    output = clean_output_powr_connect(output)

    return output

# Permet de tester en local
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        payload_file = sys.argv[1]
    else:
        payload_file = "payload_pc.json"
    with open(payload_file, "r", encoding="utf-8") as f:
        payload = json.load(f)
    output = asyncio.run(scrape_powr_connect(payload, headless=False))
    print(output)