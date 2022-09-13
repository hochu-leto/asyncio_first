from pprint import pprint

import requests


def main2():
    curr = requests.get('https://swapi.dev/api/people/').json()
    pprint(curr)
    # curr = [item['id'] for item in curr['results']]
    for i in range(curr['count']):
        r = requests.get(f'https://swapi.dev/api/people//{i}').json()

        print(f'{r} ')

    # coros = (call_api(f'https://swapi.dev/api/people/{i}', s)
    #        for i in range(curr['count']))