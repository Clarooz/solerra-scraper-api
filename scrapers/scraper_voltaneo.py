import asyncio
import pandas as pd
from playwright.async_api import async_playwright
import json
from datetime import datetime
import re

async def accept_cookies_voltaneo(page):
    """
    Accepte la bannière de cookies sur le site Voltaneo si elle est présente.
    """
    try:
        await page.locator('button.cmplz-btn.cmplz-accept').click(timeout=3000)
    except:
        pass

async def login_voltaneo(page, email, password):
    """
    Se connecte au site Voltaneo avec les identifiants fournis.
    """
    await page.goto("https://webshop.voltaneo.com/login")
    await accept_cookies_voltaneo(page)
    await page.fill('input[name="username"]', email)
    await page.fill('input[name="password"]', password)
    try:
        await page.check('input[name="rememberme"]', force=True)
    except:
        pass
    await page.locator('button:has-text("Se connecter")').click(force=True)
    await page.wait_for_url(lambda url: not url.endswith("/login/"), timeout=10000)

async def scrape_product_voltaneo(page, item):
    """
    Scrape les informations détaillées d'un produit Voltaneo à partir de son URL.
    """
    url = item["url"]
    errors = []
    data = {}

    try:
        await page.goto(url)
        await page.wait_for_load_state("domcontentloaded")
        await accept_cookies_voltaneo(page)
    except Exception as e:
        return {
            **item,
            **{f: "N/A" for f in ["name", "reference", "price_per_unit_1", "unit_1", "price_per_unit_2", "unit_2", "price_per_unit_3", "unit_3", "stock", "technical_ref"]},
            "is_ok": 0,
            "error": f"page.goto failed: {str(e)}"
        }

    try:
        name = await page.text_content('h1.product_title.entry-title')
        data["name"] = name.strip() if name else "N/A"
        if not name:
            errors.append("missing name")
    except Exception as e:
        data["name"] = "N/A"
        errors.append(f"name error: {str(e)}")

    try:
        desc = await page.text_content('div.product_description')
        data["description"] = desc.strip() if desc else "N/A"
        if not desc:
            errors.append("missing description")
    except Exception as e:
        data["description"] = "N/A"
        errors.append(f"description error: {str(e)}")

    try:
        await page.wait_for_selector('section.addToCartSection p.conditionnement', timeout=5000)
        price_elements = page.locator('section.addToCartSection p.conditionnement')
        count = await price_elements.count()

        max_prices = 3
        index = 1
        for i in range(min(count, max_prices)):
            element = price_elements.nth(i)
            if await element.is_visible():
                label = await element.locator('span.label').text_content()
                label = label.strip() if label else "N/A"
                number = await element.locator('span.number').text_content()
                number = number.strip() if number else "N/A"
                data[f"unit_{index}"] = label
                data[f"price_per_unit_{index}"] = number
                index += 1

        for i in range(index, max_prices + 1):
            data[f"unit_{i}"] = "N/A"
            data[f"price_per_unit_{i}"] = "N/A"

    except Exception as e:
        errors.append(f"price options error: {str(e)}")

    try:
        stock_text = await page.text_content('div.stock span.label')
        stock_text = stock_text.strip() if stock_text else ""
        if stock_text:
            stock_number = ""
            if await page.locator('div.stock span.number').count() > 0:
                number = await page.text_content('div.stock span.number')
                stock_number = number.strip() if number else ""
            combined = f"{stock_text} {stock_number}".strip()
            data["stock"] = combined
        else:
            data["stock"] = "N/A"
            errors.append("missing stock label")
    except Exception as e:
        data["stock"] = "N/A"
        errors.append(f"stock error: {str(e)}")

    try:
        await page.wait_for_selector('div.col', timeout=3000)
        tech_elements = await page.locator('div.col div.fcat').all_text_contents()
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

def clean_output_voltaneo(output_df):
    """
    Nettoie et enrichit le DataFrame final des résultats du scraping Voltaneo.
    """
    for col in ['price_per_unit_1', 'price_per_unit_2', 'price_per_unit_3']:
        if col in output_df.columns:
            output_df[col] = output_df[col].astype(str).apply(
                lambda x: re.findall(r'[\d,]+', x)[0] if re.findall(r'[\d,]+', x) else "N/A"
            )
            output_df[col] = output_df[col].str.replace(',', '.').astype(float, errors='ignore')

    output_df['is_available'] = output_df['stock'].apply(
        lambda x: 1 if isinstance(x, str) and 'stock' in x.lower() else 0
    )

    def clean_technical_ref(raw_list):
        cleaned = []
        if isinstance(raw_list, list):
            for item in raw_list:
                if isinstance(item, str):
                    item = item.strip().replace('\n', '').replace('\r', '')
                    item = re.sub(r'\s+', ' ', item)
                    cleaned.append(item)
        return cleaned if cleaned else "N/A"

    if 'technical_ref' in output_df.columns:
        output_df['technical_ref'] = output_df['technical_ref'].apply(clean_technical_ref)

    output_df['created_at'] = datetime.now()

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

async def scrape_voltaneo(payload, headless=True):
    """
    Lance la session Playwright et exécute le scraping pour chaque produit Voltaneo.
    """
    credentials = payload["credentials"]
    data = payload["data"]
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            await login_voltaneo(page, credentials["username"], credentials["password"])
        except:
            return [{"error": "login_failed"}]

        for item in data:
            try:
                result = await scrape_product_voltaneo(page, item)
            except Exception as e:
                result = {**item, "error": str(e), "status": "failed"}
            results.append(result)

        await browser.close()

    output = pd.DataFrame(results)
    output = clean_output_voltaneo(output)

    return output

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        payload_file = sys.argv[1]
    else:
        payload_file = "payload_v.json"
    with open(payload_file, "r", encoding="utf-8") as f:
        payload = json.load(f)
    output = asyncio.run(scrape_voltaneo(payload, headless=False))
    print(output)