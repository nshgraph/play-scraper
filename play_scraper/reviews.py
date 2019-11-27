'''
This is a python transcription of the code in the PR at https://github.com/facundoolano/google-play-scraper/pull/341/files
This should allow review pulling in python to work again (post the 08/2019 change to Google's site)
'''

import re
import copy
import json
import time
import datetime
import logging
from enum import Enum

try:
    from functools import reduce, map
except ImportError:
    pass


_log = logging.getLogger(__name__)

from play_scraper.utils import send_request

BASE_URL = 'https://play.google.com'
REQUEST_MAPPINGS = {
  "reviews": [0],
  "token": [1, 1]
}

MAX_ITERATIONS = 100

def generateDate (dateArray):
    if not dateArray:
        return None

    secondsTotal = dateArray[0]
    time =  datetime.datetime.utcfromtimestamp(int(secondsTotal))
    return time


def generateUrlFunction(appId):
    def generateUrl(reviewId):
        return "{}/store/apps/details?id={}&reviewId={}".format(BASE_URL,appId,reviewId)
    return generateUrl

def r_path(path, data):
    for p in path:
        try:
            data = data[p]
        except:
            return ""
    return data

def extractor (mappings):
    '''
    * Map the MAPPINGS object, applying each field spec to the parsed data.
    * If the mapping value is an array, use it as the path to the extract the
    * field's value. If it's an object, extract the value in object.path and pass
    * it to the function in object.fun
    '''
    def extractFields (parsedData):

        def map_spec(spec): 
            if type(mappings[spec]) == list:
                return (spec, r_path(mappings[spec], parsedData))
            
            # assume spec object
            input_data = r_path(mappings[spec]["path"], parsedData)
            return (spec,mappings[spec]["fun"](input_data))

        result = list(map(map_spec, mappings))
        out_result = {x[0]:x[1] for x in result}
        return out_result
    
    return extractFields

def parse (response):
    '''
    * Extract the javascript objects returned by the AF_initDataCallback functions
    * in the script tags of the app detail HTML.
    '''
    scriptRegex = "/>AF_initDataCallback[\s\S]*?<\/script/g"
    keyRegex = "/(ds:.*?)'/"
    valueRegex = "/return ([\s\S]*?)}}\);<\//"

    matches = re.search(scriptRegex, response)

    if not matches:
        return {}

    def reduce_function(accum, data):
        keyMatch = re.search(keyRegex, data)
        valueMatch = re.search(valueRegex, data) 

        if keyMatch and valueMatch:
            key = keyMatch[1]
            value = json.loads(valueMatch[1])
            return R.assoc(key, value, accum)
        return accum

    return reduce(reduce_function, data, {})
  
MAPPINGS = {
    "review_id": [0],
    "author_name": [1, 0],
    "author_image": [1, 1, 3, 2],
    "review_date": {
      "path": [5],
      "fun": generateDate
    },
    "current_rating": [2],
    "current_rating_text": {
      "path": [2],
      "fun": lambda x: str(x)
    },
    "review_permalink": {
      "path": [0],
      "fun": None
    },
    "review_title": {
      "path": [0],
      "fun": lambda x: ""
    },
    "review_body": [4],
    "reply_date": {
      "path": [7, 2],
      "fun": generateDate
    },
    "reply_text": {
      "path": [7, 1],
      "fun": lambda x: x
    },
    "version": {
      "path": [10],
      "fun": lambda x: x
    },
    "thumbs_up": [6],
    "criterias": {
      "path": [12, 0],
      "fun": lambda x: ""
    }
  }

# Apply MAPPINGS for each application in list from root path
def extract (root, data, appId):
    
  input_data = r_path(root, data)
  mappings = copy.deepcopy(MAPPINGS)
  mappings["review_permalink"]["fun"] = generateUrlFunction(appId)

  return list(map(extractor(mappings), input_data))


'''
 This object allow us to differ between
 the initial body request and the paginated ones
'''
class REQUEST_TYPE(Enum):
  initial = 'initial'
  paginated = 'paginated'


'''
 This method allow us to get the body for the review request
 
 @param options.appId The app id for reviews
 @param options.sort The sort order for reviews
 @param options.numberOfReviewsPerRequest The number of reviews per request
 @param options.withToken The token to be used for the given request
 @param options.requestType The
'''
def getBodyForRequests (
    appId,
    sort,
    numberOfReviewsPerRequest = 100,
    withToken = '%token%',
    requestType = REQUEST_TYPE.initial
    ):
    # The body is slight different for the initial and paginated requests */
    formBody = {
        REQUEST_TYPE.initial: "f.req=%5B%5B%5B%22UsvDTd%22%2C%22%5Bnull%2Cnull%2C%5B2%2C{}%2C%5B{}%2Cnull%2Cnull%5D%2Cnull%2C%5B%5D%5D%2C%5B%5C%22{}%5C%22%2C7%5D%5D%22%2Cnull%2C%22generic%22%5D%5D%5D".format(
            sort,
            numberOfReviewsPerRequest,
            appId
        ),
        REQUEST_TYPE.paginated: "f.req=%5B%5B%5B%22UsvDTd%22%2C%22%5Bnull%2Cnull%2C%5B2%2C{}%2C%5B{}%2Cnull%2C%5C%22{}%5C%22%5D%2Cnull%2C%5B%5D%5D%2C%5B%5C%22{}%5C%22%2C7%5D%5D%22%2Cnull%2C%22generic%22%5D%5D%5D".format(
            sort,
            numberOfReviewsPerRequest,
            withToken,
            appId
        )
    }

    return formBody[requestType]


def check_finished (opts, saved_reviews, nextToken, remaining_iterations = MAX_ITERATIONS):
    # this is where we should check that we have enough / time based check
    if ( not nextToken
        or (opts["max_records"] > 0  and len(saved_reviews) >= opts["max_records"])
        or (opts["earliest_record"] and len(saved_reviews) and saved_reviews[-1]["review_date"] < opts["earliest_record"])):
        return saved_reviews
    
    body = getBodyForRequests(
        opts["appId"],
        opts["sort"],
        withToken = nextToken,
        requestType = opts["requestType"]
        )
    headers = {
        'Content-Type': 'application/x-www-form-urlencodedcharset=UTF-8'
    }
    
    _log.info('Pulling data. Have {} records'.format(len(saved_reviews)))
    url = "{}/_/PlayStoreUi/data/batchexecute?rpcids=qnKhOb&f.sid=-697906427155521722&bl=boq_playuiserver_20190903.08_p0&hl={}&gl={}&authuser&soc-app=121&soc-platform=1&soc-device=1&_reqid=1065213".format(
        BASE_URL,
        opts["lang"],
        opts["country"]
        )
    time.sleep(0.5)
    response = send_request('POST', url, data=body, allow_redirects=True)

    input_data = json.loads(response.text[5:])
    if not input_data[0][2]:
        return saved_reviews
    data = json.loads(input_data[0][2])
    if not data:
        return saved_reviews

    if type(data) == str:
        data = scriptData.parse(data)

    reviews = extract(REQUEST_MAPPINGS["reviews"], data, opts["appId"])
    token = r_path(REQUEST_MAPPINGS["token"], data)
    opts["requestType"] = REQUEST_TYPE.paginated

    saved_reviews.extend(reviews)

    return check_finished(opts, saved_reviews, token, remaining_iterations)

def process_full_reviews (opts):
    opts["requestType"] = REQUEST_TYPE.initial
    return check_finished(opts, [], '%token%')
