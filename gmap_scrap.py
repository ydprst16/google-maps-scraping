import re
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from dataclasses import dataclass, asdict, field
import pandas as pd
import os
import itertools
import openpyxl
from datetime import datetime
import threading

# List of business categories
business_category = [
    "School", "Masjid", "Gereja", "Hotel", "Kafe", 
    "Restoran" , "Bengkel"
]

def extract_kelurahan_kecamatan(address: str) -> tuple:
    """
    Ekstrak kelurahan dan kecamatan dari alamat.
    """
    match = re.search(r"([A-Za-z\s]+),\s*(Kec\.\s*[A-Za-z\s]+)", address)
    
    if match:
        kelurahan = match.group(1)
        kecamatan = match.group(2).replace("Kec.", "").strip()  # Menghapus kata 'Kec.' langsung
        
        return kelurahan, kecamatan
    
    return None, None

@dataclass
class Business:
    """Holds business data"""
    name: str = None
    address: str = "No Address"
    kelurahan: str = None  # Menambahkan kolom kelurahan
    kecamatan: str = None  # Menambahkan kolom kecamatan
    website: str = "No Website"
    phone_number: str = "No Phone"
    rating: float = None
    latitude: float = None
    longitude: float = None

    def set_kelurahan_kecamatan(self, address: str):
        """Extract and set kelurahan and kecamatan from address."""
        self.kelurahan, self.kecamatan = extract_kelurahan_kecamatan(address)

@dataclass
class BusinessList:
    """Holds list of Business objects and saves to both Excel and CSV."""
    business_list: list = field(default_factory=list)
    save_at: str = 'output'

    seen_businesses: set = field(default_factory=set)  # Set to track unique businesses

    def dataframe(self):
        """Transform business_list to a pandas dataframe."""
        return pd.json_normalize(
            (asdict(business) for business in self.business_list), sep="_"
        )

    def save_to_csv(self, filename, append=True, business_type=None):
        """Saves pandas dataframe to a CSV file."""
        # Create the directory if it doesn't exist
        os.makedirs(self.save_at, exist_ok=True)

        current_time = datetime.now().strftime("%Y-%m-%d")
        file_name = f"{filename}_{business_type}_{current_time}" if business_type else f"{filename}_{current_time}"
        file_path = os.path.join(self.save_at, f"{file_name}.csv")
    
        # Choose write mode
        mode = 'a' if append and os.path.exists(file_path) else 'w'

        # Save dataframe to CSV with appropriate header settings
        self.dataframe().to_csv(file_path, mode=mode, index=False, header=not append or not os.path.exists(file_path))

    def save_to_excel(self, filename, business_type):
        """Saves pandas dataframe to an Excel file and auto-fits columns."""
        os.makedirs(self.save_at, exist_ok=True)

        current_time = datetime.now().strftime("%Y-%m-%d")
        excel_path = os.path.join(self.save_at, f"{filename}_{business_type}_{current_time}.xlsx")
        self.dataframe().to_excel(excel_path, index=False)

        # Open Excel and auto-fit columns
        wb = openpyxl.load_workbook(excel_path)
        ws = wb.active

        for col in ws.columns:
            max_length = max(len(str(cell.value)) for cell in col if cell.value)
            ws.column_dimensions[col[0].column_letter].width = max_length + 2

        wb.save(excel_path)

    def add_business(self, business):
        """Add a business to the list if it's not a duplicate."""
        unique_key = (business.name, business.address, business.phone_number)
        if unique_key not in self.seen_businesses:
            self.seen_businesses.add(unique_key)
            self.business_list.append(business)
            return True  # Business was added
        else:
            return False  # Business was a duplicate
    
    def __init__(self, save_at='output'):
        self.business_list = []
        self.save_at = save_at
        self.seen_businesses = set()
        self.auto_save_running = False  # Flag untuk auto-save thread

    def auto_save(self, filename, business_type):
        """Simpan data setiap 1 menit meskipun scraping belum selesai."""
        if self.business_list:  # Hanya simpan jika ada data baru
            print("\n[Auto-Save] Saving progress...")
            self.save_to_excel(filename, business_type)
            self.save_to_csv(filename, business_type=business_type)
        
        # Jadwalkan penyimpanan otomatis setiap 60 detik
        self.auto_save_thread = threading.Timer(60, self.auto_save, args=(filename, business_type))
        self.auto_save_thread.daemon = True  # Pastikan thread berhenti ketika program selesai
        self.auto_save_thread.start()

    def stop_auto_save(self):
        """Hentikan proses auto-save saat scraping selesai."""
        if hasattr(self, 'auto_save_thread'):
            self.auto_save_thread.cancel()


def extract_coordinates_from_url(url: str) -> tuple:
    """Extracts coordinates from URL."""
    try:
        coordinates = url.split('/@')[-1].split('/')[0].split(',')
        return float(coordinates[0]), float(coordinates[1])
    except (IndexError, ValueError) as e:
        print(f"Error extracting coordinates: {e}")
        return None, None

def clean_business_name(name: str) -> str:
    """Remove '· Visited link' from the business name."""
    return name.replace(" · Visited link", "").strip()

def spinning_cursor():
    spinner = itertools.cycle(['|', '/', '-', '\\'])
    while True:
        yield f"\033[91m{next(spinner)}\033[0m"  # Red-colored spinner using ANSI escape codes

def main():
    # Display menu for business categories
    print("Select one or more business types by entering their numbers separated by commas or ranges (e.g., 1,3,5-7):")
    for i, business in enumerate(business_category, start=1):
        print(f"{i}. {business}")

    # Get user input for business categories
    business_input = input("Enter the number(s) of the business categories you want to scrape (e.g., 1,3-5): ")

    # Function to parse the input
    def parse_business_input(business_input):
        selected_indices = set()
        for part in business_input.split(','):
            part = part.strip()
            if '-' in part:
                start, end = part.split('-')
                start = int(start.strip()) - 1  # Adjust for 0-based index
                end = int(end.strip()) - 1
                selected_indices.update(range(start, end + 1))
            else:
                index = int(part.strip()) - 1
                selected_indices.add(index)
        return sorted(selected_indices)

    selected_business_indices = parse_business_input(business_input)

    selected_business_category = [business_category[i] for i in selected_business_indices if 0 <= i < len(business_category)]

    if not selected_business_category:
        print("No valid business types selected.")
        return

    # Ask user if they want to run in headless mode
    headless_choice = input("Do you want to run the script in headless mode? (y/n): ").strip().lower()
    headless = headless_choice == 'y'

    centralized_filename = "gmaps_data"

    spinner = spinning_cursor()

    # Ask user for the number of listings to scrape per business type
    num_listings_to_capture = int(input(f"How many listings do you want to scrape for each business type? "))

    # Initialize BusinessList
    business_list = BusinessList()

    # Inisialisasi penyimpanan otomatis setiap 1 menit
    business_list.auto_save(centralized_filename, selected_business_category[0])

    ###########
    # scraping
    ###########
    # Begin scraping process
    with sync_playwright() as p:
        # Start browser in headless mode based on user input
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()

        for selected_business_type in selected_business_category:
            print(f"\nScraping for business type: {selected_business_type}")

            # Reset listings_scraped for this business type
            listings_scraped = 0

            # Define a fixed list with only one city: Dumai, Riau
            cities_states_original = [("Dumai", "Riau")]

            # Iterate through cities_states_original, which now only contains Dumai
            for selected_city, selected_state in cities_states_original:
                print(f"Searching for {selected_business_type} in {selected_city}, {selected_state}.")
                search_for = f"{selected_business_type} in {selected_city}, {selected_state}"

                # Try to perform the search on Google Maps
                try:
                    page.goto("https://www.google.com/maps", timeout=30000)
                    page.wait_for_selector('//input[@id="searchboxinput"]', timeout=10000)
                    page.locator('//input[@id="searchboxinput"]').fill(search_for)
                    page.keyboard.press("Enter")
                    page.wait_for_selector('//a[contains(@href, "https://www.google.com/maps/place")]', timeout=7000)
                except PlaywrightTimeoutError as e:
                    print(f"Timeout error occurred while searching for {selected_business_type} in {selected_city}: {e}")
                    continue
                except Exception as e:
                    print(f"Error occurred while searching for {selected_business_type} in {selected_city}: {e}")
                    continue

                try:
                    current_count = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').count()
                except Exception as e:
                    print(f"Error detecting results for {selected_city}, skipping: {e}")
                    continue

                if current_count == 0:
                    print(f"No results found for {selected_business_type} in {selected_city}, {selected_state}. Moving to next city.")
                    continue

                print(f"Found {current_count} listings for {selected_business_type} in {selected_city}, {selected_state}.")

                # Scroll through listings and wait for the elements to load
                MAX_SCROLL_ATTEMPTS = 10
                scroll_attempts = 0
                previously_counted = current_count

                # Loop to scrape listings until reaching the target number of listings
                while listings_scraped < num_listings_to_capture:
                    try:
                        listings = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').all()
                    except Exception as e:
                        print(f"Error while fetching listings: {e}")
                        break

                    if not listings:
                        print(f"No more listings found. Moving to the next city.")
                        break

                    # Loop through the listings and scrape the data
                    for listing in listings:
                        try:
                            if listings_scraped >= num_listings_to_capture:
                                break

                            spinner_char = next(spinner)
                            print(f"\rScraping listing: {listings_scraped + 1} of {num_listings_to_capture} {spinner_char}", end='')

                            # Pastikan elemen listing terlihat sebelum diklik
                            #listing.scroll_into_view_if_needed()

                            # Retry clicking the listing in case of an issue
                            MAX_CLICK_RETRIES = 5
                            for retry_attempt in range(MAX_CLICK_RETRIES):
                                try:
                                    #listing.click()
                                    listing.click(force=True)
                                    page.wait_for_timeout(2000)
                                    break
                                except Exception as e:
                                    print(f"Retrying click, attempt {retry_attempt + 1}: {e}")
                                    page.wait_for_timeout(1000)

                            # Define locators to extract business information
                            name_attribute = 'aria-label'
                            address_xpath = 'xpath=//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
                            website_xpath = 'xpath=//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
                            phone_number_xpath = 'xpath=//button[contains(@data-item-id, "phone:tel:")]//div[contains(@class, "fontBodyMedium")]'
                            rating_xpath = '//div[@jsaction="pane.reviewChart.moreReviews"]//div[@role="img"]'

                            # Define the details panel to scope our locators
                            details_panel = page.locator('div[role="main"]')

                            # Create a Business object to store the extracted data
                            business = Business()

                            # Extract data from the page and assign to the Business object
                            business.name = clean_business_name(listing.get_attribute(name_attribute)) if listing.get_attribute(name_attribute) else "Unknown"
                            business.address = page.locator(address_xpath).first.inner_text() if page.locator(address_xpath).count() > 0 else "No Address"
                            business.website = page.locator(website_xpath).first.inner_text() if page.locator(website_xpath).count() > 0 else "No Website"
                            business.phone_number = page.locator(phone_number_xpath).first.inner_text() if page.locator(phone_number_xpath).count() > 0 else "No Phone"

                            # Ekstrak kelurahan dan kecamatan
                            business.set_kelurahan_kecamatan(business.address)

                            # Extract rating
                            rating_element = details_panel.locator(rating_xpath).first
                            if rating_element.count() > 0:
                                rating_text = rating_element.get_attribute('aria-label')
                                if rating_text:
                                    match = re.search(r'(\d+\.\d+|\d+)', rating_text.replace(',', '.'))
                                    if match:
                                        business.rating = float(match.group(1))
                                    else:
                                        business.rating = 0.0
                                else:
                                    business.rating = 0.0
                            else:
                                business.rating = 0.0

                            business.latitude, business.longitude = extract_coordinates_from_url(page.url)

                            added = business_list.add_business(business)
                            if added:
                                listings_scraped += 1

                        except Exception as e:
                            print(f"\nError occurred while scraping listing: {e}")
                            continue  # Continue to the next listing

                        if listings_scraped >= num_listings_to_capture:
                            break

                    page.mouse.wheel(0, 5000)
                    page.wait_for_timeout(3000)

                    new_count = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').count()
                    if new_count == previously_counted:
                        scroll_attempts += 1
                        if scroll_attempts >= MAX_SCROLL_ATTEMPTS:
                            print(f"No more listings found after {scroll_attempts} scroll attempts. Moving to next city.")
                            break
                    else:
                        scroll_attempts = 0

                    previously_counted = new_count

                    if page.locator("text=You've reached the end of the list").is_visible():
                        print(f"Reached the end of the list in {selected_city}, {selected_state}. Moving to the next city.")
                        break

            #########
            # output
            #########
            # Save any remaining businesses after finishing all business types
            if business_list.business_list:
                business_list.save_to_excel(centralized_filename, selected_business_type)
                business_list.save_to_csv(centralized_filename, business_type=selected_business_type)
                business_list.business_list.clear()

        # Hentikan penyimpanan otomatis setelah scraping selesai
        business_list.stop_auto_save()

        browser.close()

if __name__ == "__main__":
    main()
