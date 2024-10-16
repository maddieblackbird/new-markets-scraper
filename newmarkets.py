import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from urllib.parse import urljoin, urlparse, urldefrag
import time
from datetime import datetime, timedelta

def normalize_url(url):
    """Normalize URL by removing fragments and trailing slashes, and converting to lowercase."""
    url = urldefrag(url)[0]  # Remove fragment
    url = url.rstrip('/')    # Remove trailing slash
    url = url.lower()        # Convert to lowercase
    return url

def get_eater_links(start_url, max_articles=5):  # Limit to 5 articles for testing
    print(f"Starting to crawl Eater URLs from {start_url}")
    visited = set()
    to_visit = [start_url]
    eater_urls = []
    cutoff_date = datetime.now() - timedelta(days=547.5)  # Approximately 1.5 years

    while to_visit:
        if len(eater_urls) >= max_articles:
            print(f"Reached the maximum number of articles ({max_articles}). Stopping crawl.")
            break
        url = to_visit.pop()
        url_normalized = normalize_url(url)
        print(f"Processing URL: {url}")
        if url_normalized in visited:
            print(f"Skipping URL (already visited): {url}")
            continue
        visited.add(url_normalized)
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract publication date
            time_tag = soup.find('time', class_='c-byline__item', attrs={'data-ui': 'timestamp'})
            if time_tag and 'datetime' in time_tag.attrs:
                article_date_str = time_tag['datetime']
                try:
                    article_date = datetime.fromisoformat(article_date_str)
                    if article_date < cutoff_date:
                        print(f"Article is older than 1.5 years: {article_date}")
                        continue  # Skip this article
                except ValueError:
                    print(f"Invalid date format in article: {article_date_str}")
                    continue

            # Find all links that start with the desired prefixes
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'tel:' in href:
                    print(f"Skipping telephone link: {href}")
                    continue  # Skip links with 'tel:'
                full_url = urljoin(url, href)
                full_url_normalized = normalize_url(full_url)
                if (
                    full_url_normalized.startswith('https://dallas.eater.com/2023') or
                    full_url_normalized.startswith('https://dallas.eater.com/2024') or
                    full_url_normalized.startswith('https://dallas.eater.com/maps')
                ):
                    if full_url_normalized not in visited and full_url_normalized not in to_visit:
                        to_visit.append(full_url)
                        print(f"Found new URL to visit: {full_url}")

            # Collect URLs that contain restaurant data (heuristic)
            if ('/maps/' in url or '/article/' in url) and url not in eater_urls:
                eater_urls.append(url)
                print(f"Added URL to Eater restaurant URLs: {url}")

        except requests.RequestException as e:
            print(f"Failed to fetch {url}: {e}")

    print(f"Finished crawling Eater URLs. Total URLs found: {len(eater_urls)}")
    return eater_urls

def extract_eater_restaurants(url, processed_urls):
    url_normalized = normalize_url(url)
    if url_normalized in processed_urls:
        print(f"URL already processed: {url}")
        return []
    print(f"\nExtracting restaurants from Eater URL: {url}")
    processed_urls.add(url_normalized)
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        restaurants = []

        # Extract publication date
        time_tag = soup.find('time', class_='c-byline__item', attrs={'data-ui': 'timestamp'})
        if time_tag and 'datetime' in time_tag.attrs:
            article_date_str = time_tag['datetime']
            try:
                article_date = datetime.fromisoformat(article_date_str)
                cutoff_date = datetime.now() - timedelta(days=547.5)  # 1.5 years
                if article_date < cutoff_date:
                    print(f"Article is older than 1.5 years: {article_date}")
                    return []  # Skip this article
            except ValueError:
                print(f"Invalid date format in article: {article_date_str}")
                return []

        # Find all restaurant sections
        sections = soup.find_all('section', class_='c-mapstack__card')
        print(f"Found {len(sections)} restaurant sections on the page.")
        for section in sections:
            name_tag = section.find('h1')
            address_tag = section.find('div', class_='c-mapstack__address')
            phone_tag = section.find('div', class_='c-mapstack__phone')
            website_tag = section.find('a', attrs={'data-analytics-link': 'link-icon'})

            name = name_tag.get_text(strip=True) if name_tag else None
            address = address_tag.get_text(strip=True) if address_tag else None
            phone = phone_tag.get_text(strip=True) if phone_tag else None
            website = website_tag['href'] if website_tag else None

            if name:
                restaurants.append({
                    'Name': name,
                    'Address': address,
                    'Phone': phone,
                    'Website': website
                })
                print(f"Extracted restaurant: {name}")

        return restaurants

    except requests.RequestException as e:
        print(f"Error extracting from {url}: {e}")
        return []

def get_infatuation_links(start_url, max_articles=5):
    print(f"\nStarting to crawl The Infatuation URLs from {start_url}")
    visited = set()
    to_visit = [start_url]
    infatuation_urls = []
    cutoff_date = datetime.now() - timedelta(days=547.5)  # Approximately 1.5 years
    desired_prefix = 'https://www.theinfatuation.com/dallas/guides'

    while to_visit:
        if len(infatuation_urls) >= max_articles:
            print(f"Reached the maximum number of articles ({max_articles}). Stopping crawl.")
            break
        url = to_visit.pop()
        url_normalized = normalize_url(url)
        print(f"Processing URL: {url}")
        if url_normalized in visited:
            print(f"Skipping URL (already visited): {url}")
            continue
        visited.add(url_normalized)
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract publication date
            time_tag = soup.find('time')
            if time_tag and 'datetime' in time_tag.attrs:
                article_date_str = time_tag['datetime']
                try:
                    article_date = datetime.fromisoformat(article_date_str)
                    if article_date < cutoff_date:
                        print(f"Article is older than 1.5 years: {article_date}")
                        continue  # Skip this article
                except ValueError:
                    print(f"Invalid date format in article: {article_date_str}")
                    continue

            # Find all links that start with the desired prefix
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'tel:' in href:
                    print(f"Skipping telephone link: {href}")
                    continue  # Skip links with 'tel:'
                full_url = urljoin(url, href)
                full_url_normalized = normalize_url(full_url)
                # Only include URLs that start with the desired prefix
                if full_url_normalized.startswith(desired_prefix):
                    if full_url_normalized not in visited and full_url_normalized not in to_visit:
                        to_visit.append(full_url)
                        print(f"Found new URL to visit: {full_url}")

            # Collect URLs that might contain restaurant data
            if url_normalized.startswith(desired_prefix) and url not in infatuation_urls:
                infatuation_urls.append(url)
                print(f"Added URL to Infatuation restaurant URLs: {url}")

        except requests.RequestException as e:
            print(f"Failed to fetch {url}: {e}")

    print(f"Finished crawling Infatuation URLs. Total URLs found: {len(infatuation_urls)}")
    return infatuation_urls

def extract_infatuation_restaurants(url, processed_urls):
    url_normalized = normalize_url(url)
    if url_normalized in processed_urls:
        print(f"URL already processed: {url}")
        return []
    print(f"\nExtracting restaurants from Infatuation URL: {url}")
    processed_urls.add(url_normalized)
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        restaurants = []

        # Extract publication date
        time_tag = soup.find('time')
        if time_tag and 'datetime' in time_tag.attrs:
            article_date_str = time_tag['datetime']
            try:
                article_date = datetime.fromisoformat(article_date_str)
                cutoff_date = datetime.now() - timedelta(days=547.5)  # 1.5 years
                if article_date < cutoff_date:
                    print(f"Article is older than 1.5 years: {article_date}")
                    return []  # Skip this article
            except ValueError:
                print(f"Invalid date format in article: {article_date_str}")
                return []

        # Find all restaurant sections (heuristic based on HTML structure)
        venue_cards = soup.find_all('div', class_=re.compile('styles_venueCard__'))
        print(f"Found {len(venue_cards)} venue cards on the page.")
        for card in venue_cards:
            name_tag = card.find('h2')
            address_tag = card.find('a', attrs={'data-testid': 'venue-googleMapUrl'})
            phone_tag = card.find('a', attrs={'data-testid': 'venue-phoneNumber'})
            website_tag = card.find('a', attrs={'data-testid': 'venue-url'})

            name = name_tag.get_text(strip=True) if name_tag else None
            address = address_tag.get_text(strip=True) if address_tag else None
            phone = phone_tag.get_text(strip=True) if phone_tag else None
            website = website_tag['href'] if website_tag else None

            if name:
                restaurants.append({
                    'Name': name,
                    'Address': address,
                    'Phone': phone,
                    'Website': website
                })
                print(f"Extracted restaurant: {name}")

        return restaurants

    except requests.RequestException as e:
        print(f"Error extracting from {url}: {e}")
        return []

def combine_and_deduplicate(eater_data, infatuation_data):
    print(f"\nCombining data from Eater ({len(eater_data)} entries) and Infatuation ({len(infatuation_data)} entries)")
    df_eater = pd.DataFrame(eater_data)
    df_infatuation = pd.DataFrame(infatuation_data)

    combined_df = pd.concat([df_eater, df_infatuation], ignore_index=True)
    before_dedup = len(combined_df)
    combined_df.drop_duplicates(subset=['Website'], inplace=True)
    after_dedup = len(combined_df)
    print(f"Combined data has {before_dedup} entries before deduplication, {after_dedup} after deduplication")
    return combined_df

def extract_emails(text):
    """Extract and return valid email addresses from text."""
    # Use regex to find all potential emails within the text
    # This pattern matches sequences containing '@', allowing preceding and following characters
    potential_emails = re.findall(r'\S+@\S+', text)

    valid_emails = set()
    for email in potential_emails:
        # Clean up the email by removing any non-email characters at the start and end
        email = re.sub(r'^[^\w]+', '', email)  # Remove non-word characters at the start
        email = re.sub(r'[^\w]+$', '', email)  # Remove non-word characters at the end
        # Extract the actual email from any concatenated text
        match = re.search(r'[\w\.-]+@[\w\.-]+\.\w{2,}', email)
        if match:
            email = match.group(0)
            if is_valid_email(email):
                valid_emails.add(email)
    return valid_emails

def is_valid_email(email):
    """Validate email addresses based on custom rules."""
    # Exclude emails with unwanted domains
    unwanted_domains = [
        'domain.com', 'latofonts.com', 'wixpress.com', 'example.com',
        'yourdomain.com', 'placeholder.com', 'email.com', 'sentry.wixpress.com', 'sentry-next.wixpress.com'
    ]
    # Exclude emails with unwanted prefixes
    unwanted_prefixes = [
        'user@', 'team@', 'info@mysite.com', 'noreply@', 'no-reply@',
        'support@', 'admin@', 'webmaster@', 'test@', 'example@'
    ]

    email_lower = email.lower()
    domain = email_lower.split('@')[-1]

    # Check for unwanted domains
    if domain in unwanted_domains:
        return False

    # Check for unwanted prefixes
    if any(email_lower.startswith(prefix) for prefix in unwanted_prefixes):
        return False

    # Exclude emails that are too long or too short
    if not (5 <= len(email) <= 254):
        return False

    # Exclude emails with non-ASCII characters
    if not all(ord(c) < 128 for c in email):
        return False

    # Exclude emails with consecutive digits (e.g., phone numbers)
    if re.search(r'\d{5,}', email):
        return False

    # Basic email format validation
    email_regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    if not re.match(email_regex, email):
        return False

    return True

def scrape_emails_and_platforms_from_website(url, max_links=5):
    emails = set()
    visited_links = set()  # Track visited links to avoid duplicates
    pos_systems = set()
    reservation_platforms = set()

    # Define patterns for POS systems and reservation platforms
    pos_patterns = {
        'Toast': re.compile(r'toasttab\.com'),
        'Square': re.compile(r'squareup\.com|square\.site'),
        'Clover': re.compile(r'clover\.com'),
        'ShopKeep': re.compile(r'shopkeep\.com'),
        'Revel Systems': re.compile(r'revelsystems\.com'),
        'Upserve': re.compile(r'upserve\.com'),
        'TouchBistro': re.compile(r'touchbistro\.com'),
        'Lightspeed': re.compile(r'lightspeedhq\.com'),
        # Add more POS systems as needed
    }

    reservation_patterns = {
        'OpenTable': re.compile(r'opentable\.com'),
        'Resy': re.compile(r'resy\.com'),
        'Tock': re.compile(r'exploretock\.com'),
        'Yelp Reservations': re.compile(r'yelp\.com/reservations'),
        'SevenRooms': re.compile(r'sevenrooms\.com'),
        # Add more reservation platforms as needed
    }

    print(f"\nScraping emails and platforms from website: {url}")
    try:
        url_normalized = normalize_url(url)
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        text = soup.get_text()
        html_content = response.text

        # Extract emails
        new_emails = extract_emails(text)
        emails.update(new_emails)
        if new_emails:
            print(f"Found emails on main page: {new_emails}")

        # Search for POS systems and reservation platforms in HTML content
        for name, pattern in pos_patterns.items():
            if pattern.search(html_content):
                pos_systems.add(name)
                print(f"Found POS system '{name}' on main page.")

        for name, pattern in reservation_patterns.items():
            if pattern.search(html_content):
                reservation_platforms.add(name)
                print(f"Found reservation platform '{name}' on main page.")

        visited_links.add(url_normalized)
        # Check for links to other pages and scrape them up to max_links
        links_scraped = 1
        for link in soup.find_all('a', href=True):
            if links_scraped >= max_links:
                break
            href = link['href']
            if 'tel:' in href:
                print(f"Skipping telephone link: {href}")
                continue  # Skip links with 'tel:'
            absolute_link = urljoin(url, href)
            absolute_link_normalized = normalize_url(absolute_link)
            parsed_link = urlparse(absolute_link_normalized)

            # Only follow links within the same domain
            if parsed_link.netloc == urlparse(url_normalized).netloc and absolute_link_normalized not in visited_links:
                try:
                    link_response = requests.get(absolute_link, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
                    link_response.raise_for_status()
                    link_text = link_response.text
                    link_soup = BeautifulSoup(link_response.content, 'html.parser')
                    link_text_content = link_soup.get_text()

                    # Extract emails
                    new_emails = extract_emails(link_text_content)
                    if new_emails:
                        emails.update(new_emails)
                        print(f"Found emails on page {absolute_link}: {new_emails}")

                    # Search for POS systems and reservation platforms
                    for name, pattern in pos_patterns.items():
                        if pattern.search(link_text):
                            pos_systems.add(name)
                            print(f"Found POS system '{name}' on page {absolute_link}.")

                    for name, pattern in reservation_patterns.items():
                        if pattern.search(link_text):
                            reservation_platforms.add(name)
                            print(f"Found reservation platform '{name}' on page {absolute_link}.")

                    visited_links.add(absolute_link_normalized)
                    links_scraped += 1
                    time.sleep(1)  # Be polite and avoid overwhelming the server
                except requests.RequestException:
                    continue

        return emails, pos_systems, reservation_platforms

    except requests.RequestException as e:
        print(f"Error scraping {url}: {e}")
        return emails, pos_systems, reservation_platforms

def update_with_emails_and_platforms(df, max_websites=50):
    print("\nStarting to extract emails and platforms from restaurant websites.")
    emails_list = []
    pos_list = []
    reservation_list = []
    processed_websites = set()  # Track processed websites
    websites_processed_count = 0  # Counter for unique websites processed
    for index, row in df.iterrows():
        if websites_processed_count >= max_websites:
            print(f"Reached the maximum number of websites ({max_websites}). Stopping extraction.")
            # Append None for the remaining restaurants
            remaining = len(df) - len(emails_list)
            emails_list.extend([None] * remaining)
            pos_list.extend([None] * remaining)
            reservation_list.extend([None] * remaining)
            break
        print(f"\nProcessing restaurant {index + 1}/{len(df)}: {row['Name']}")
        website = row['Website']
        if pd.notnull(website):
            website_normalized = normalize_url(website)
            if website_normalized in processed_websites:
                print(f"Website already processed: {website}")
                emails_list.append(None)
                pos_list.append(None)
                reservation_list.append(None)
                continue
            emails, pos_systems, reservation_platforms = scrape_emails_and_platforms_from_website(website)
            emails_list.append(', '.join(emails) if emails else None)
            pos_list.append(', '.join(pos_systems) if pos_systems else None)
            reservation_list.append(', '.join(reservation_platforms) if reservation_platforms else None)
            print(f"Extracted emails from {website}: {emails}")
            print(f"Detected POS systems: {pos_systems}")
            print(f"Detected reservation platforms: {reservation_platforms}")
            processed_websites.add(website_normalized)
            websites_processed_count += 1
        else:
            emails_list.append(None)
            pos_list.append(None)
            reservation_list.append(None)
            print(f"No website URL for {row['Name']}")
    df['Emails'] = emails_list
    df['POS Systems'] = pos_list
    df['Reservation Platforms'] = reservation_list
    print("Finished extracting emails and platforms.")
    return df

def save_to_csv(df, filename='restaurants.csv'):
    df.to_csv(filename, index=False)
    print(f"\nData saved to {filename}")

def main():
    # Eater URLs
    eater_start_url = 'https://dallas.eater.com/'
    print("Starting to process Eater data.")
    eater_links = get_eater_links(eater_start_url, max_articles=5)  # Limit to 5 articles for testing
    eater_restaurants = []
    processed_urls = set()
    for link in eater_links:
        eater_restaurants.extend(extract_eater_restaurants(link, processed_urls))
        time.sleep(1)  # Be polite

    print(f"\nExtracted total {len(eater_restaurants)} restaurants from Eater.")

    # Infatuation URLs
    infatuation_start_url = 'https://www.theinfatuation.com/dallas/guides'
    print("\nStarting to process The Infatuation data.")
    infatuation_links = get_infatuation_links(infatuation_start_url, max_articles=5)  # Limit to 5 articles for testing
    infatuation_restaurants = []
    for link in infatuation_links:
        infatuation_restaurants.extend(extract_infatuation_restaurants(link, processed_urls))
        time.sleep(1)  # Be polite

    print(f"\nExtracted total {len(infatuation_restaurants)} restaurants from The Infatuation.")

    # Combine and deduplicate
    combined_df = combine_and_deduplicate(eater_restaurants, infatuation_restaurants)

    # Extract emails and platforms (limited to 50 unique restaurant websites)
    combined_df = update_with_emails_and_platforms(combined_df, max_websites=50)

    # Save to CSV
    save_to_csv(combined_df)

if __name__ == "__main__":
    main()
