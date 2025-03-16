import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from webdriver_manager.firefox import GeckoDriverManager

def download_w_letters():
    # ✅ Set up default and custom download paths
    custom_download_dir = os.path.abspath("./data")  # Custom save directory
    os.makedirs(custom_download_dir, exist_ok=True)  # Create folder if missing

    # ✅ Define the final filename for the most updated version
    final_filename = "warning_letters.xlsx"
    final_filepath = os.path.join(custom_download_dir, final_filename)

    # ✅ Function to clean up old files before downloading
    def clean_old_files():
        if os.path.exists(final_filepath):
            os.remove(final_filepath)
            print(f"Old file {final_filename} deleted before downloading new version.")

    # ✅ Set up Firefox WebDriver
    def create_firefox_driver():
        options = FirefoxOptions()
        options.set_preference("browser.download.folderList", 2)  # Use custom folder
        options.set_preference("browser.download.dir", custom_download_dir)  # Set download location
        options.set_preference("browser.helperApps.neverAsk.saveToDisk", 
                            "application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        service = FirefoxService(GeckoDriverManager().install())
        return webdriver.Firefox(service=service, options=options)

    # ✅ Initialize Firefox WebDriver
    driver = create_firefox_driver()

    # ✅ Open the webpage
    url = "https://www.fda.gov/inspections-compliance-enforcement-and-criminal-investigations/compliance-actions-and-activities/warning-letters"
    driver.get(url)
    time.sleep(5)  # Wait for page to load

    # ✅ Detect existing files before clicking the download button
    existing_files = set(os.listdir(custom_download_dir))

    # ✅ Remove old file before downloading a new one
    clean_old_files()

    # ✅ Click the Export Excel button
    try:
        export_button = driver.find_element(By.XPATH, "//button[contains(@class, 'excel-export')]")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", export_button)
        time.sleep(1)  # Ensure visibility
        export_button.click()
        print("Export button clicked. Waiting for download...")
    except Exception as e:
        print(f"Error: {e}")

    # ✅ Function to wait for new file and rename it
    def wait_for_download(directory, timeout=60):
        start_time = time.time()
        while time.time() - start_time < timeout:
            files = set(os.listdir(directory))
            new_files = files - existing_files  # Detect newly downloaded file
            for file in new_files:
                if file.endswith(".xlsx") and not file.endswith(".part"):  # Ensure fully downloaded
                    old_path = os.path.join(directory, file)
                    os.rename(old_path, final_filepath)
                    print(f"Download complete! Renamed to: {final_filename}")
                    return True
            time.sleep(5)
        print("Download timed out.")
        return False

    # ✅ Call the function to wait for and rename the file
    wait_for_download(custom_download_dir)

    # ✅ Keep the browser open until the file is fully downloaded
    while not os.path.exists(final_filepath):
        print("Waiting for file to be renamed correctly...")
        time.sleep(5)

    print(f"File saved as: {final_filepath}")

    # ✅ Close browser after confirmation
    driver.quit()

download_w_letters()