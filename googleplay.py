import datetime as dt
import pandas as pd
import tzlocal
import time
import random
import google_play_scraper
from google_play_scraper import app, Sort, reviews
from pprint import pprint
from tzlocal import get_localzone

country_mapping = {
    'ae': 'United Arab Emirates',
    'ar': 'Argentina',
    'au': 'Australia',
    'ca': 'Canada',
    'cn': 'China',
    'de': 'Germany',
    'fr': 'France',
    'hk': 'Hong Kong',
    'in': 'India',
    'ru': 'Russia',
    'sa': 'Saudi Arabia',
    'se': 'Sweden',
    'tr': 'Turkey',
    'us': 'United States',
    'za': 'South Africa',
}
country_names = list(country_mapping.keys())

scores_to_scrape = [1,2,5]

languages = ['en', 'tr']

app_names = [
    'duolingo',
    'cambly',
    'busuu',
    'memrise',
    'mycake',
    'openenglish',
    'babbel',
    'atistudios'
]

app_ids = [
    'com.duolingo',
    'com.cambly.cambly',
    'com.busuu.android.enc',
    'com.memrise.android.memrisecompanion',
    'me.mycake',
    'com.openenglish',
    'com.babbel.mobile.android.en',
    'com.atistudios.mondly.languages'
]


# Number of reviews to scrape per batch
batch_size=200
# Number of batches
num_batches=5000


def write_reviews_to_csv(reviews, package_name, score_to_scrape):
    file_name = f'{package_name}_score_{score_to_scrape}_reviews.csv'
    df = pd.DataFrame(reviews)
    headerList = ['reviewId', 'userName','userImage', 'content', 'score', 'thumbsUpCount', 'reviewCreatedVersion','at', 'replyContent', 'repliedAt', 'appVersion', 'lang', 'country', 'app_name', 'app_id' ]
    df.to_csv(file_name, mode='a', index=False, header=headerList,encoding='utf_8_sig')
    print(f'{len(reviews)} reviews written to {file_name}')


def scrape_reviews(app_name, app_id, score_to_scrape, start):
    # Empty list for storing reviews
    app_reviews = []
    # Empty list for storing review ids - to eliminate duplicates
    app_reviews_ids = []

    for country in country_names:
        for lang in languages:
            # To keep track of how many batches have been completed
            batch_num = 0

            # Retrieve reviews (and continuation_token) with reviews function
            rvws, token = reviews(
                app_id,  # found in app's url
                lang=lang,  # defaults to 'en'
                country=country,  # defaults to 'us'
                sort=Sort.NEWEST,  # start with most recent
                count=batch_size,  # batch size
                filter_score_with=score_to_scrape  # review score
            )

            # For each review obtained
            for r in rvws:
                r['language'] = lang  # add key for the language
                r['country'] = country  # add key for the country
                r['app_name'] = app_name  # add key for app's name
                r['app_id'] = app_id  # add key for app's id
                if r['reviewId'] not in app_reviews_ids:
                    app_reviews.append(r)  # Add the list of review dicts to overall list
                    app_reviews_ids.append(r['reviewId'])  # Append review IDs to list prior to starting next batch

            # app_reviews.extend(rvws)

            # Increase batch count by one
            batch_num += 1
            #print(f'Batch {batch_num} completed.')

            # Wait 1 to 5 seconds to start next batch
            time.sleep(random.randint(1, 5))

            # Loop through at most max number of batches
            for batch in range(num_batches):
                # for batch in range(4999):
                rvws, token = reviews(  # store continuation_token
                    app_id,
                    lang=lang,
                    country=country,
                    sort=Sort.NEWEST,
                    count=batch_size,
                    # using token obtained from previous batch
                    continuation_token=token,
                    filter_score_with=score_to_scrape  # review score
                )

                # Append unique review IDs from current batch to new list
                new_review_ids = []
                for r in rvws:
                    # And add keys for name and id to ea review dict
                    r['language'] = lang  # add key for the language
                    r['country'] = country  # add key for the country
                    r['app_name'] = app_name  # add key for app's name
                    r['app_id'] = app_id  # add key for app's id
                    if r['reviewId'] not in app_reviews_ids:
                        app_reviews.append(r)  # Add the list of review dicts to overall list
                        app_reviews_ids.append(r['reviewId'])  # Append review IDs to list prior to starting next batch
                        new_review_ids.append(r['reviewId'])

                # Increase batch count by one
                batch_num += 1

                # Break loop and stop scraping for current app if most recent batch
                # did not add any unique reviews
                if len(new_review_ids) == 0:
                    #print(f'No reviews left to scrape. Completed {batch_num} batches.\n')
                    break

            # Print update when max number of batches has been reached
            # OR when last batch didn't add any unique reviews
            #print(f'Done scraping {app_name} for score {score_to_scrape} for country {country} for language {lang}')
            # Get end time
            end = dt.datetime.now(tz=get_localzone())

    # Print ending output for app
    print(f'Successfully inserted {len(app_reviews_ids)} reviews with score {score_to_scrape} for {app_name} into collection at {end.strftime(fmt)}.')
    print(f'Time elapsed for {app_name}: {end - start}')

    write_reviews_to_csv(app_reviews, app_name, score_to_scrape)
    # Wait 1 to 5 seconds to start scraping next app
    time.sleep(random.randint(1, 5))


## Loop through apps to get reviews
for app_name, app_id in zip(app_names, app_ids):
    for score in scores_to_scrape:
        # Get start time
        start = dt.datetime.now(tz=get_localzone())
        fmt= "%m/%d/%y - %T %p"
    
        # Print starting output for app
        print('---'*20)
        print('---'*20)
        print(f'***** {app_name} for score {score} started at {start.strftime(fmt)}')

        scrape_reviews(app_name, app_id, score, start)




