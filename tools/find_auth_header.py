import time
from seleniumwire import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.firefox import GeckoDriverManager

# --- CONFIGURATION ---
LOGIN_URL = "https://lnppass.legapallacanestro.com/"
PLAYLIST_DOMAINS = [
    'dvr-i-01-lnp-ei.akamaized.net',  # HLS domain
    'dvr-d-01-lnp-ei.akamaized.net'   # DASH domain
]

print("--- Starting Selenium WebDriver (Diagnostic Mode) ---")
print("A new Firefox window will open. Please log in to your account normally.")
print("After you log in, navigate to the video you want to save AND PRESS PLAY.")
print("The script will monitor network traffic in the background...")

service = Service(GeckoDriverManager().install())
driver = webdriver.Firefox(service=service)

driver.get(LOGIN_URL)

found_request = None

try:
    print("\n--- WAITING FOR LOGIN AND VIDEO PLAYBACK ---")
    print(f"Monitoring domains: {', '.join(PLAYLIST_DOMAINS)}")
    print("Press play on your video to begin detection...\n")

    while not found_request:
        for request in driver.requests:
            is_playlist_domain = any(domain in request.host for domain in PLAYLIST_DOMAINS)
            is_manifest = ".m3u8" in request.url or ".mpd" in request.url

            if is_playlist_domain and is_manifest and request.response:
                # We found the playlist request! Capture it.
                found_request = request
                break
        
        if found_request:
            break
        
        time.sleep(0.5)

    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("!!! SUCCESS: PLAYLIST REQUEST CAPTURED !!!")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")

    print("You can now close the Firefox window. The process is done.")
    print("The captured stream URL is below. This is what you paste into the control panel.")
    
    print("\n--- [Captured Stream URL] ---")
    print(f'"{found_request.url}"')
    
    print("\n--- [Full Request Headers (for debugging)] ---")
    # Print all headers neatly
    for header_name, header_value in found_request.headers.items():
        print(f'"{header_name}: {header_value}"')

    print("\n")


except Exception as e:
    print(f"An error occurred: {e}")
    print("If you closed the browser, please re-run the script.")
finally:
    print("Script finished. You can close this terminal.")
    driver.quit()
