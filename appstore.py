from app_store_scraper import AppStore
import pandas as pd
import time
import logging
from requests.exceptions import SSLError
from urllib3.exceptions import MaxRetryError
from langdetect import detect

review_number = 12000
apps = [
         {"id": 987873536, "name": "mondly", "app_name": "Mondly"},
]

country_mapping = {
    'il': 'Israel',
    'ae': 'United Arab Emirates',
    'ar': 'Argentina',
    'au': 'Australia',
    'br': 'Brazil',
    'ca': 'Canada',
    'cl': 'Chile',
    'cn': 'China',
    'de': 'Germany',
    'dk': 'Denmark',
    'fr': 'France',
    'gl': 'Greenland',
    'hk': 'Hong Kong',
    'ie': 'Ireland',
    'in': 'India',
    'ir': 'Iran',
    'it': 'Italy',
    'jp': 'Japan',
    'kr': 'South Korea',
    'mx': 'Mexico',
    'nl': 'Netherlands',
    'no': 'Norway',
    'nz': 'New Zealand',
    'ru': 'Russia',
    'sa': 'Saudi Arabia',
    'se': 'Sweden',
    'tr': 'Turkey',
    'uk': 'United Kingdom',
    'us': 'United States',
    'fi': 'Finland',
    'es': 'Spain',
    'et': 'Ethiopia',
    'za': 'South Africa',
    'jo': 'Jordan',
    'lb': 'Lebanon'
}

languages = ['en', 'tr']
ratings_to_scrape = [1, 2, 5]
logging.basicConfig(level=logging.INFO)

def filter_reviews_by_language(reviews, language):
    language_reviews = []
    for review in reviews:
        content = review.get("review")
        if content and len(content) > 20:  # Only consider reviews with more than 20 characters
            try:
                if detect(content) == language:
                    if language == 'en':
                        content = clean_text(content)  # Clean up English reviews
                    review['review'] = content
                    review['Language'] = language
                    language_reviews.append(review)
            except Exception as e:
                logging.error(f"Error detecting language: {e}")
                continue
    return language_reviews

def clean_text(text):
    replacements = {
        "â€™": "'",
        "â€": "",
        # Add more replacements as needed
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text

def fetch_reviews(app_store, max_retries=5):
    retries = 0
    reviews_fetched = 0
    while retries < max_retries and reviews_fetched < review_number:
        try:
            app_store.review(how_many=review_number - reviews_fetched, after=None, sleep=1)
            reviews_fetched += len(app_store.reviews)
            return app_store.reviews[:review_number]  # Ensure not to exceed review_number
        except MaxRetryError as e:
            if '429' in str(e):
                wait = 2 ** retries
                logging.warning(f"Rate limit hit. Waiting for {wait} seconds.")
                time.sleep(wait)
                retries += 1
            else:
                logging.error("Max retries exceeded with url.")
                raise
        except SSLError as e:
            logging.error(f"SSL error occurred: {e}")
            raise
    return []

for app_info in apps:
    app_id = app_info["id"]
    app_name = app_info["app_name"]
    for rating in ratings_to_scrape:
        all_reviews = []
        for country_code in country_mapping.keys():
            app_store = AppStore(country=country_code, app_id=app_id, app_name=app_name)
            reviews = fetch_reviews(app_store)
            for language in languages:
                language_reviews = filter_reviews_by_language(reviews, language)
                for review in language_reviews:
                    review['Country'] = country_mapping[country_code]
                    review['Rating'] = rating
                all_reviews.extend(language_reviews)
        
        reviews_df = pd.DataFrame(all_reviews).drop_duplicates(subset=['review', 'Country', 'Rating'])
        file_name = f'{app_name}-rating-{rating}-reviews.csv'
        reviews_df.to_csv(file_name, index=False, header=True, encoding='utf_8_sig')
        logging.info(f'Combined reviews for {app_name} with rating {rating} saved to {file_name}')
