import asyncio
from playwright.async_api import async_playwright
import nest_asyncio

nest_asyncio.apply()

async def dump_all():
    url = "https://boaters-boatrace.com/race/fukuoka/2026-04-23/4R"
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            
            tabs = ["直前情報", "連対率・展開", "AI予想"]
            for tab in tabs:
                try:
                    await page.wait_for_timeout(1000)
                    await page.get_by_text(tab, exact=True).click(timeout=5000)
                    await page.wait_for_timeout(2000)
                    text = await page.evaluate("() => document.body.innerText")
                    filename = f"dump_{tab}.txt"
                    with open(filename, "w") as f:
                        f.write(text)
                    print(f"Dumped {tab} to {filename}")
                except Exception as e:
                    print(f"Error dumping {tab}: {e}")
                    
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(dump_all())
