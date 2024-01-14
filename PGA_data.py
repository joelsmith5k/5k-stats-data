import pymongo
import requests
from constants import *
from datetime import datetime


DATABASE = pymongo.MongoClient(MONGO_CONNECTION_STRING)[PGA_DB_NAME]
PLAYER_COLLECTION = DATABASE[PGA_PLAYER_COLLECTION]
TOURNEY_COLLECTION = DATABASE[PGA_TOURNAMENT_COLLECTION]
CURRENT_YEAR = '2024'
PREVIOUS_YEAR = '2023'
# TODAY = str(datetime.now())[:10]
TODAY = '2024-01-10'


def api_call(url):
    """Call api using private key as header in HTTP request."""
    response = requests.get(url, headers=PGA_HEADERS)
    return response.json()


def get_tournament():
    print(TODAY)
    tournament_data = api_call(PGA_TOURNAMENT_BASE_URL + CURRENT_YEAR)
    filtered = list(filter(lambda x: x['StartDate'] >= TODAY, tournament_data))
    print(filtered[-1])
    next_tournament = filtered[-1]
    TOURNEY_COLLECTION.delete_many({})
    TOURNEY_COLLECTION.insert_one(next_tournament)
    return next_tournament


def get_projections(tournament_id):
    projection_data = api_call(PGA_PROJECTIONS_BASE_URL + tournament_id)
    filtered_data = list(filter(lambda x: x['Rank'] is not None, projection_data))
    sorted_data = sorted(filtered_data, key=lambda x: x['Rank'])
    for player in sorted_data:
        player['name'] = player['Name']
    PLAYER_COLLECTION.delete_many({})
    PLAYER_COLLECTION.insert_many(sorted_data)
    return sorted_data


def main():
    next_tournament = get_tournament()
    print(next_tournament)
    stats = get_projections(str(next_tournament['TournamentID']))
    print(stats)


if __name__ == "__main__":
    main()
