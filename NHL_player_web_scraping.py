from bs4 import BeautifulSoup
from constants import *
import requests
from requests_ip_rotator import ApiGateway
import csv


gateway = ApiGateway("https://www.hockey-reference.com/", access_key_id=AWS_ACCESS_KEY,
                     access_key_secret=AWS_SECRET_ACCESS_KEY)
gateway.start()

SESSION = requests.Session()
FIELDS = ['name', 'pos', 'gp', 'g', 'a', 'p', 'p/m', 's', 'spct',  'atoi']
ROW_INDICES = [0, 2, 3, 4, 5, 6, 7, 16, 17, 19]


def main():
    SESSION.mount('https://www.hockey-reference.com/', gateway)

    team = 'VAN'
    year = '2024'
    url = f'https://www.hockey-reference.com/teams/{team}/{year}.html'
    page = SESSION.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    all_skaters = soup.find(id="all_skaters")
    table = all_skaters.find("tbody")
    rows = table.find_all("tr")

    players = []

    # one row for each player
    for row in rows:
        row_data = row.find_all("td")
        filtered_data = list(map(lambda x: row_data[x], ROW_INDICES))
        player = {}
        for i in range(len(FIELDS)):
            player[FIELDS[i]] = filtered_data[i].text
        players.append(player)

    filename = f"skating_stats_{team}.csv"
    # writing to csv file
    with open(filename, 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(players)

    # REMEMBER TO SHUT GATEWAY DOWN
    gateway.shutdown()


if __name__ == "__main__":
    main()
