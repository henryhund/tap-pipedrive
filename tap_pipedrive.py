import argparse
import requests
import singer
import json
import os
# import singer.stats

session = requests.Session()
logger = singer.get_logger()

parser = argparse.ArgumentParser()

parser.add_argument(
    '-c', '--config', help='Config file', required=True)

args = parser.parse_args()

with open(args.config) as config_file:
    config = json.load(config_file)

api_token = config['api-token']

auth = "?api_token=" + api_token



def authed_get(source, url):
    # with singer.stats.Timer(source=source) as stats:
    resp = session.request(method='get', url=url)
    # stats.http_status_code = resp.status_code
    return resp

def authed_get_all_pages(source, url):
    while True:
        r = authed_get(source, url)
        yield r
        if r.json()['additional_data']['pagination']['more_items_in_collection']:
            url = url +'&start='+str(r.json()['additional_data']['pagination']['next_start'])
        else:
            break

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# TO DO: update schemas

def load_schemas():
    schemas = {}

    with open(get_abs_path('tap_github/commits.json')) as file:
        schemas['commits'] = json.load(file)

    with open(get_abs_path('tap_github/issues.json')) as file:
        schemas['issues'] = json.load(file)

    return schemas

def get_all_deals(state):
    if 'deals' in state and state['deals'] is not None:
        query_string = '&sort=id%20DESC&limit=500'
    else:
        query_string = '&sort=id%20DESC&limit=500'

    latest_deal_time = None

    # with singer.stats.Counter(source='deals') as stats:
    url = 'https://api.pipedrive.com/v1/deals{}{}'.format(auth, query_string)
    deal_count = 0
    for response in authed_get_all_pages('deals', url ):
            deals = response.json()['data']
            # logger.info('URL: %s ', url)
            # logger.info('Deals: %s ', deals)
            
            for deal in deals:
                # stats.add(record_count=1)
                
                deal_id = str(deal.pop('id', None))
                deal_count = deal_count + 1
                # if deal.get('update_time') >= state.get('deals'):
                    
                singer.write_record('deals', deal)
                    

                # elif state['deals'] is None:
                #     singer.write_records('deal', deals)
                # else:
                #     break
            if not latest_deal_time:
                latest_deal_time = deals[0]['update_time']
    logger.info(deal_count)

    state['deal'] = latest_deal_time
    return state

def do_sync(config, state):
    # access_token = config['access_token']
    # repo_path = config['repository']
    # schemas = load_schemas()
    
    # session.headers.update({'authorization': 'token ' + access_token})

    if state:
        logger.info('Replicating deals since %s ', state)
    else:
        logger.info('Replicating deals ')

        
    # singer.write_schema('commits', schemas['commits'], 'sha')
    # singer.write_schema('issues', schemas['issues'], 'id')
    state = get_all_deals(state)
    # state = get_all_issues(repo_path, state)
    singer.write_state(state)


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument(
        '-s', '--state', help='State file')

    args = parser.parse_args()

    with open(args.config) as config_file:
        config = json.load(config_file)

    missing_keys = []
    for key in ['api-token']:
        if key not in config:
            missing_keys += [key]

    if len(missing_keys) > 0:
        logger.fatal("Missing required configuration keys: {}".format(missing_keys))
        exit(1)

    state = {}
    if args.state:
        with open(args.state, 'r') as file:
            for line in file:
                state = json.loads(line.strip())

    do_sync(config, state)
    
if __name__ == '__main__':
    main()

