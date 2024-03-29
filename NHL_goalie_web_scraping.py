from datetime import date, timedelta
from bs4 import BeautifulSoup, NavigableString, Tag
from constants import *
import pymongo
import requests
from requests_ip_rotator import ApiGateway
from sample_nhl_data import OVERWRITE_DATA_GAME_IDS, UPDATE_GAME_IDS


# CONSTANTS
DATABASE = pymongo.MongoClient(MONGO_CONNECTION_STRING)[NHL_DB_NAME]
GOALIE_COLLECTION = DATABASE[NHL_GOALIE_COLLECTION]
AGGREGATE_COLLECTION = DATABASE[NHL_AGGREGATE_COLLECTION]

gateway = ApiGateway("https://www.hockey-reference.com", access_key_id=AWS_ACCESS_KEY,
                     access_key_secret=AWS_SECRET_ACCESS_KEY)
gateway.start()

SESSION = requests.Session()

BOX_SCORES_URL = ('https://www.hockey-reference.com/boxscores/index.fcgi?'
                  'month=%s&day=%s&year=%s')

GOALIE_IDS = ["allenja01", "anderfr01", "binnijo01", "blackma01", "blackma01", "bobrose01", "brossla01", "campbja01",
              "copleph01", "demkoth01", "desmica01", "dostalu01", "fleurma01", "forsban01", "georgal01", "gibsojo02",
              "grubaph01", "gustafi01", "hartca01", "helleco01", "hillad01", "hussovi01", "jarrytr01", "johanjo03",
              "jonesma02", "kahkoka01", "korpijo01", "kuempda01", "levide01", "luukkuk01", "marksja02", "merzlel01",
              "montesa01", "mrazepe01", "murrama02", "oettija01","quickjo01", "raantan01", "reimeja01", "samsoil01",
              "sarosju01", "schmiak01", "shestig01", "skinnje01", "skinnst01", "sorokil01", "swaymje01", "talboca01",
              "thomplo01", "ullmali01", "vanecvi01", "varlasi01", "vasilan02", "vejmeka01", "wedgesc01", "wolljo01"]

# GLOBAL VARIABLES
global_goalies = {}
global_players = {}
aggregates = {"GA": 0, "HL_GA": 0, "HR_GA": 0, "PL_GA": 0, "PR_GA": 0, "PC_GA": 0, "PD_GA": 0,
              "HLPL_GA": 0, "HLPR_GA": 0, "HLPC_GA": 0, "HLPD_GA": 0, "HRPL_GA": 0, "HRPR_GA": 0,
              "HRPC_GA": 0, "HRPD_GA": 0, "num_players_HL": 0, "num_players_HR": 0, "num_players_PL": 0,
              "num_players_PC": 0, "num_players_PR": 0, "num_players_PD": 0, "num_players_HLPL": 0,
              "num_players_HRPL": 0, "num_players_HLPR": 0, "num_players_HRPR": 0, "num_players_HLPC": 0,
              "num_players_HRPC": 0, "num_players_HLPD": 0, "num_players_HRPD": 0}


# HELPER FUNCTIONS
def format_days():
    days = []
    start_date = date(2019, 11, 1)
    end_date = date(2020, 4, 1)
    increment = timedelta(days=+1)
    while start_date < end_date:
        days.append(start_date)
        start_date = start_date + increment
    return days


def generate_game_ids(list_of_days):
    game_ids = []
    for i in range(len(list_of_days)):
        game_day = list_of_days[i]
        url = BOX_SCORES_URL % (str(game_day.month), str(game_day.day), str(game_day.year))
        games_page = SESSION.get(url)
        page_soup = BeautifulSoup(games_page.content, "html.parser")
        games_info = page_soup.findAll("table", class_="teams")
        for game in games_info:
            box_score_link = game.find("td", class_="right gamelink")
            box_score_uri = box_score_link.find("a")
            game_id = (str(box_score_uri)[20:32])
            game_ids.append(game_id)
    return game_ids


def set_goalie_names():
    for goalie in global_goalies.keys():
        global_goalies[goalie]["Name"] = get_player_info(goalie)[-2]


def finalize_player_aggregates():
    player_list = list(global_players.values())
    left_handed = list(filter(lambda x: x['dexterity'] == "left", player_list))
    right_handed = list(filter(lambda x: x['dexterity'] == "right", player_list))
    aggregates["num_players_HL"] = len(left_handed)
    aggregates["num_players_HR"] = len(right_handed)
    aggregates["num_players_PL"] = len(list(filter(lambda x: x['position'] == "lw", player_list)))
    aggregates["num_players_PC"] = len(list(filter(lambda x: x['position'] == "c", player_list)))
    aggregates["num_players_PR"] = len(list(filter(lambda x: x['position'] == "rw", player_list)))
    aggregates["num_players_PD"] = len(list(filter(lambda x: x['position'] == "d", player_list)))
    aggregates["num_players_HLPL"] = len(list(filter(lambda x: x['position'] == "lw", left_handed)))
    aggregates["num_players_HLPC"] = len(list(filter(lambda x: x['position'] == "c", left_handed)))
    aggregates["num_players_HLPR"] = len(list(filter(lambda x: x['position'] == "rw", left_handed)))
    aggregates["num_players_HLPD"] = len(list(filter(lambda x: x['position'] == "d", left_handed)))
    aggregates["num_players_HRPL"] = len(list(filter(lambda x: x['position'] == "lw", right_handed)))
    aggregates["num_players_HRPC"] = len(list(filter(lambda x: x['position'] == "c", right_handed)))
    aggregates["num_players_HRPR"] = len(list(filter(lambda x: x['position'] == "rw", right_handed)))
    aggregates["num_players_HRPD"] = len(list(filter(lambda x: x['position'] == "d", right_handed)))


# FUNCTIONS
def update_goalie_counts(goalie_id, counts):
    if goalie_id in global_goalies:
        for key in filter(lambda x: (x != "goal_details"), global_goalies[goalie_id].keys()):
            global_goalies[goalie_id][key] += counts[key]
    else:
        global_goalies[goalie_id] = counts


def update_goalie_goal_details(goalie_id, goalscorers):
    for gs in goalscorers:
        if gs['Name'] in global_goalies[goalie_id]['goal_details'].keys():
            global_goalies[goalie_id]['goal_details'][gs['Name']]['goals'] += gs['goals']
        else:
            global_goalies[goalie_id]['goal_details'][gs['Name']] = {"team": gs['team'],
                                                                     "Name": gs['Name'],
                                                                     "goals": gs["goals"],
                                                                     "position": gs['position'],
                                                                     "dexterity": gs['dexterity']}


def get_player_info(goalscorer):
    try:
        letter = str(goalscorer[0])
        url = f"https://www.hockey-reference.com/players/{letter}/{goalscorer}.html"
        player_page = SESSION.get(url)
        page_soup = BeautifulSoup(player_page.content, "html.parser")
        player_info = page_soup.find(id="meta")
        player_info_array = str(player_info.find_all("p")[0]).split()
        player_team_array = str(player_info.find_all("p")[2]).split()
        if player_team_array[2][7:13] == "teams/":
            team = player_team_array[2][13:16]
        else:
            team = ""
        player_name = player_info.find_all("span")[0].text
        player_info_array.append(player_name)
        player_info_array.append(team)
    except AttributeError:
        return ""
    else:
        return player_info_array


def breakdown_scorers(goalscorers):
    game_players = []
    for gs in goalscorers:
        player_id = gs['player_id']
        if player_id in global_players.keys():
            game_players.append({"goals": gs['goals'], "position": global_players[player_id]["position"],
                                 "dexterity": global_players[player_id]["dexterity"],
                                 "Name": global_players[player_id]["Name"],
                                 "team": global_players[player_id]["team"]})
        else:
            data = get_player_info(player_id)
            if len(data) < 6:
                break
            else:
                position = data[1].strip().lower()
                dexterity = data[4].strip().lower()
                name = data[-2]
                team = data[-1]
                game_players.append({"goals": gs['goals'], "position": position, "dexterity": dexterity,
                                     "Name": name, "team": team})
                global_players[player_id] = {"position": position, "dexterity": dexterity, "Name": name, "team": team}
    return game_players


def count_statistics(goalscorers, goalie_id):
    counts = {"GA": 0, "HL_GA": 0, "HR_GA": 0, "PL_GA": 0, "PR_GA": 0, "PC_GA": 0, "PD_GA": 0, "HLPL_GA": 0,
              "HLPR_GA": 0, "HLPC_GA": 0, "HLPD_GA": 0, "HRPL_GA": 0, "HRPR_GA": 0, "HRPC_GA": 0, "HRPD_GA": 0,
              "goal_details": {}}
    scorers = breakdown_scorers(goalscorers)
    valid_goal_details = filter(lambda x: x['position'] in ["lw", "c", "rw", "d"], scorers)
    for gd in valid_goal_details:
        goals = gd["goals"]
        position = gd["position"]
        dexterity = gd["dexterity"]
        counts["GA"] += goals
        aggregates["GA"] += goals
        if dexterity == "left":
            counts["HL_GA"] += goals
            aggregates["HL_GA"] += goals
            if position == "lw":
                counts["PL_GA"] += goals
                counts["HLPL_GA"] += goals
                aggregates["PL_GA"] += goals
                aggregates["HLPL_GA"] += goals
            elif position == "c":
                counts["PC_GA"] += goals
                counts["HLPC_GA"] += goals
                aggregates["PC_GA"] += goals
                aggregates["HLPC_GA"] += goals
            elif position == "rw":
                counts["PR_GA"] += goals
                counts["HLPR_GA"] += goals
                aggregates["PR_GA"] += goals
                aggregates["HLPR_GA"] += goals
            elif position == "d":
                counts["PD_GA"] += goals
                counts["HLPD_GA"] += goals
                aggregates["PD_GA"] += goals
                aggregates["HLPD_GA"] += goals
        else:
            counts["HR_GA"] += goals
            aggregates["HR_GA"] += goals
            if position == "lw":
                counts["PL_GA"] += goals
                counts["HRPL_GA"] += goals
                aggregates["PL_GA"] += goals
                aggregates["HRPL_GA"] += goals
            elif position == "c":
                counts["PC_GA"] += goals
                counts["HRPC_GA"] += goals
                aggregates["PC_GA"] += goals
                aggregates["HRPC_GA"] += goals
            elif position == "rw":
                counts["PR_GA"] += goals
                counts["HRPR_GA"] += goals
                aggregates["PR_GA"] += goals
                aggregates["HRPR_GA"] += goals
            elif position == "d":
                counts["PD_GA"] += goals
                counts["HRPD_GA"] += goals
                aggregates["PD_GA"] += goals
                aggregates["HRPD_GA"] += goals
    update_goalie_counts(goalie_id, counts)
    update_goalie_goal_details(goalie_id, scorers)


def analyze_team(goalie_team, scoring_team):
    # Only breakdown opponent goals if we track the goalie
    goalscorers = []
    for player in goalie_team:
        if player in GOALIE_IDS:
            goalie_id = player
            for scorer in [s for s in scoring_team if scoring_team[s] > 0]:
                goalscorers.append({'player_id': scorer, 'goals': scoring_team[scorer]})
            if len(goalscorers) > 0:
                count_statistics(goalscorers, goalie_id)
            break


def get_team_player_info_from_html(player_rows):
    player_dict = {}
    for tr in list(player_rows):
        if isinstance(tr, NavigableString):
            continue
        if isinstance(tr, Tag):
            player = tr.find(lambda tag: tag.name == "td" and tag['data-stat'] == "player")
            goals = tr.find(lambda tag: tag.name == "td" and tag['data-stat'] == "goals")
            player_dict[player['data-append-csv']] = int(goals.text)
    return player_dict


def analyze_game(game_id):
    url = f'https://www.hockey-reference.com/boxscores/{game_id}.html'
    page = SESSION.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    tables = soup.find_all("tbody")
    if len(tables) >= 2:
        away_players = get_team_player_info_from_html(tables[0])
        home_players = get_team_player_info_from_html(tables[2])
        if len(away_players) > 0 and len(home_players) > 0:
            analyze_team(away_players, home_players)
            analyze_team(home_players, away_players)


def mongo_overwrite():
    goalie_stats = list(global_goalies.values())
    GOALIE_COLLECTION.delete_many({})
    GOALIE_COLLECTION.insert_many(goalie_stats)
    AGGREGATE_COLLECTION.delete_many({})
    AGGREGATE_COLLECTION.insert_one(aggregates)


def mongo_update():
    goalie_stats = list(global_goalies.values())
    for goalie in goalie_stats:
        mongo_document = GOALIE_COLLECTION.find_one({'hockey_ref_id': goalie["hockey_ref_id"]})
        GOALIE_COLLECTION.find_one_and_update({'hockey_ref_id': goalie["hockey_ref_id"]},
                                              {'$set': {"GA": mongo_document['GA'] + goalie['GA'],
                                                        "HL_GA": mongo_document['HL_GA'] + goalie['HL_GA'],
                                                        "HR_GA": mongo_document['HR_GA'] + goalie['HR_GA'],
                                                        "PL_GA": mongo_document['PL_GA'] + goalie['PL_GA'],
                                                        "PC_GA": mongo_document['PC_GA'] + goalie['PC_GA'],
                                                        "PR_GA": mongo_document['PR_GA'] + goalie['PR_GA'],
                                                        "PD_GA": mongo_document['PD_GA'] + goalie['PD_GA'],
                                                        "HLPL_GA": mongo_document['HLPL_GA'] + goalie['HLPL_GA'],
                                                        "HLPR_GA": mongo_document['HLPR_GA'] + goalie['HLPR_GA'],
                                                        "HLPC_GA": mongo_document['HLPC_GA'] + goalie['HLPC_GA'],
                                                        "HLPD_GA": mongo_document['HLPD_GA'] + goalie['HLPD_GA'],
                                                        "HRPL_GA": mongo_document['HRPL_GA'] + goalie['HRPL_GA'],
                                                        "HRPR_GA": mongo_document['HRPR_GA'] + goalie['HRPR_GA'],
                                                        "HRPC_GA": mongo_document['HRPC_GA'] + goalie['HRPC_GA'],
                                                        "HRPD_GA": mongo_document['HRPD_GA'] + goalie['HRPD_GA'],
                                                        # add the update goalies make sure to update this
                                                        "goal_details_2023_Q2": goalie['goal_details']
                                                        }})


def main():
    SESSION.mount("https://www.hockey-reference.com", gateway)

    # GENERATE GAME IDS FOR A DATE RANGE
    # game_ids = generate_game_ids(format_days())
    # for game in game_ids:
    #     print('"' + game + '",')

    num_games = len(OVERWRITE_DATA_GAME_IDS)
    aggregates["num_games"] = num_games
    print(f"Analyzing {num_games} games..")
    for i in range(num_games):
        print(f"Analyzing game: {OVERWRITE_DATA_GAME_IDS[i]}")
        analyze_game(OVERWRITE_DATA_GAME_IDS[i])

    aggregates["num_players"] = len(global_players)
    set_goalie_names()

    finalize_player_aggregates()
    print(global_goalies)

    # SELECT MONGO OPERATION
    mongo_operation = input("Select desired database operation.\n"
                            "O - Overwrite\n"
                            "U - Update\n"
                            "Any Other Character - Exit\n")

    if mongo_operation.lower() == 'o':
        mongo_overwrite()

    elif mongo_operation.lower() == 'u':
        mongo_update()
    else:
        print("Program exiting with no database operation.")

    # REMEMBER TO SHUT GATEWAY DOWN
    gateway.shutdown()


if __name__ == "__main__":
    main()
