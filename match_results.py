from bs4 import BeautifulSoup
import requests
import sqlite3
import json
import time

def reset_database():
    cur.executescript('''
    DROP TABLE IF EXISTS match_results;
    DROP TABLE IF EXISTS bowling_summary;
    DROP TABLE IF EXISTS batting_summary;
    DROP TABLE IF EXISTS player_info;

    CREATE TABLE match_results(
        team1 TEXT,
        team2 TEXT,
        winner TEXT,
        margin TEXT,
        ground TEXT,
        match_date TEXT,
        scorecard TEXT
    );

    CREATE TABLE bowling_summary(
        match TEXT,
        bowling_team TEXT,
        bowler TEXT,
        overs FLOAT,
        maiden INT,
        runs INT,
        wickets INT,
        economy FLOAT,
        zeros INT,
        fours INT,
        sixes INT,
        wides INT,
        no_balls INTS,
        match_id TEXT
    );

    CREATE TABLE batting_summary(
        match TEXT,
        batting_team TEXT,
        batting_pos INT,
        batter TEXT,
        runs INT,
        balls INT,
        fours INT,
        sixes INT,
        sr FLOAT,
        dismissal TEXT,
        match_id TEXT
    );
    
    CREATE TABLE player_info(
        name TEXT,
        team TEXT,
        image TEXT,
        batting_style TEXT,
        bowling_style TEXT,
        playing_role TEXT,
        description TEXT,
        education TEXT,
        link TEXT UNIQUE
    );
    ''')

def get_match_results():
    match_results = []
    # Website
    url = 'https://stats.espncricinfo.com/ci/engine/records/team/match_results.html?id=14450;type=tournament'
    html_text = requests.get(url).text
    time.sleep(1)

    # Local file
    with open('match_results.html', 'r') as html_text2:

        soup = BeautifulSoup(html_text, 'lxml')

        # Idenify table, rows, and cells, and extract data to variables
        table = soup.find_all('tr', class_='data1')
        for row in table:
            cell = row.find_all('td')
            
            team1 = cell[0].text
            team2 = cell[1].text
            winner = cell[2].text
            margin = cell[3].text
            ground = cell[4].text
            match_date = cell[5].text
            scorecard = cell[6].text
            match_link = 'https://stats.espncricinfo.com/' + cell[6].a.get('href')

            # print(f'Team 1: {team1}')
            # print(f'Team 2: {team2}')
            # print(f'Winner: {winner}')
            # print(f'Margin: {margin}')
            # print(f'Ground: {ground}')
            # print(f'Match Date: {match_date}')
            # print(f'Scorecard: {scorecard}')
            # print(f'Match link: {match_link}')
            # print('--------------------------------')

            if winner != 'abandoned':
                get_bowling_summary(match_link, scorecard)
            # Add data to database
            cur.execute('''INSERT OR IGNORE INTO match_results (team1, team2, winner, margin, ground, match_date, scorecard)
                VALUES (?, ?, ?, ?, ?, ?, ?)''', (team1, team2, winner, margin, ground, match_date, scorecard))
            
            # Create json dictionary
            dict = {
                'team1' : team1,
                'team2' : team2,
                'winner' : winner,
                'margin' : margin,
                'match_date' : match_date,
                'scorecard' : scorecard
            }

            match_results.append(dict)
            conn.commit()
    

def get_bowling_summary(url, match_id):
    # Website
    html_text = requests.get(url).text
    time.sleep(1)

    # Local file
    with open('bowling_summary.html', 'r') as html_text2:
        soup = BeautifulSoup(html_text, 'lxml')

        # Identify team 1, team 2, and match info
        teams = []
        spans = soup.find_all('span', class_='ds-text-tight-xs')
        for span in spans:
            if 'Innings' in span.text:
                teams.append(span.text.replace(' Innings',''))
        team1 = teams[0]
        team2 = teams[1]
        match_info = team1 + ' Vs ' + team2

        # Identify first and second inning tables
        tables = soup.find_all('table', class_='ds-w-full ds-table ds-table-md ds-table-auto')
        first_inning_table = tables[0]
        second_inning_table = tables[1]

        # Extract data from first inning table
        for sub_table in first_inning_table:
            if len(sub_table) > 1:
                for row in sub_table:
                    if len(row) == 11:
                        cell = row.find_all('td')

                        bowler = cell[0].text
                        bowling_team = team1
                        overs = cell[1].text
                        maiden = cell[2].text
                        runs = cell[3].text
                        wickets = cell[4].text
                        economy = cell[5].text
                        zeros = cell[6].text
                        fours = cell[7].text
                        sixes = cell[8].text
                        wides = cell[9].text
                        no_balls = cell[10].text

                        # Get player link
                        player_href = cell[0].a.get('href')
                        player_link = 'https://www.espncricinfo.com' + player_href

                        # print(f'Match Info: {match_info}')
                        # print(f'Player Link: {player_link}')
                        # print(f'Bowling Team: {bowling_team}')
                        # print(f'Bowler: {bowler}')
                        # print(f'Overs: {overs}')
                        # print(f'Maiden: {maiden}')
                        # print(f'Runs: {runs}')
                        # print(f'Wickets: {wickets}')
                        # print(f'Economy: {economy}')
                        # print(f'Zeros: {zeros}')
                        # print(f'Fours: {fours}')
                        # print(f'Sixes: {sixes}')
                        # print(f'Wides: {wides}')
                        # print(f'No Balls: {no_balls}')
                        # print(f'Match ID: {match_id}')
                        
                        # Add data to database
                        cur.execute('''INSERT OR IGNORE INTO bowling_summary (match, bowling_team, bowler, overs, maiden, runs, wickets, economy, zeros, fours, sixes, wides, no_balls, match_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (match_info, bowling_team, bowler, overs, maiden, runs, wickets, economy, zeros, fours, sixes, wides, no_balls, match_id))

                        # Get first inning bowling player info
                        get_player_info(player_link)
                        
                        

        # Extract data from second inning table
        for sub_table in second_inning_table:
            if len(sub_table) > 1:
                for row in sub_table:
                    if len(row) == 11:
                        cell = row.find_all('td')

                        bowler = cell[0].text
                        bowling_team = team2
                        overs = cell[1].text
                        maiden = cell[2].text
                        runs = cell[3].text
                        wickets = cell[4].text
                        economy = cell[5].text
                        zeros = cell[6].text
                        fours = cell[7].text
                        sixes = cell[8].text
                        wides = cell[9].text
                        no_balls = cell[10].text

                        # Get player link
                        player_href = cell[0].a.get('href')
                        player_link = 'https://www.espncricinfo.com' + player_href

                        # Add data to database
                        cur.execute('''INSERT OR IGNORE INTO bowling_summary (match, bowling_team, bowler, overs, maiden, runs, wickets, economy, zeros, fours, sixes, wides, no_balls, match_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (match_info, bowling_team, bowler, overs, maiden, runs, wickets, economy, zeros, fours, sixes, wides, no_balls, match_id))

                        # Get second inning bowling player info
                        get_player_info(player_link)
                        
        get_batting_summary(soup, match_info, team1, team2, match_id)
        

def get_batting_summary(soup, match_info, team1, team2, match_id):

    tables = soup.find_all('table', class_='ds-w-full ds-table ds-table-md ds-table-auto ci-scorecard-table')

    # Identify first and second inning tables
    first_inning_table = tables[0]
    second_inning_table = tables[1]

    # Extract data from first inning table
    for sub_table in first_inning_table:
        if len(sub_table) > 1:
            i = 0
            for row in sub_table:
                if len(row) == 8:
                    i += 1
                    cell = row.find_all('td')

                    batter = cell[0].text.replace('Â', '')
                    batting_team = team1
                    batting_pos = i
                    runs = cell[2].text
                    balls = cell[3].text
                    fours = cell[5].text
                    sixes = cell[6].text
                    sr = cell[7].text
                    if cell[1].text == 'not out ':
                        dismissal = 'not out'
                    else:
                        dismissal = 'out'
                    
                    # Get player link
                    player_href = cell[0].a.get('href')
                    player_link = 'https://www.espncricinfo.com' + player_href

                    # print(f'Match Info: {match_info}')
                    # print(f'Batter: {batter}')
                    # print(f'Batting Team: {batting_team}')
                    # print(f'Batting Position: {batting_pos}')
                    # print(f'Runs: {runs}')
                    # print(f'Balls: {balls}')
                    # print(f'Fours: {fours}')
                    # print(f'Sixes: {sixes}')
                    # print(f'SR: {sr}')
                    # print(f'Dismissal: {dismissal}')
                    # print(f'Match ID: {match_id}')
                    
                    # Add data to database
                    cur.execute('''INSERT OR IGNORE INTO batting_summary (match, batting_team, batter, batting_pos, runs, balls, fours, sixes, sr, dismissal, match_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (match_info, batting_team, batter, batting_pos, runs, balls, fours, sixes, sr, dismissal, match_id))
    
                    # Get first inning batting player info
                    get_player_info(player_link)
                    
    # Extract data from second inning table
    for sub_table in second_inning_table:
        if len(sub_table) > 1:
            i = 0
            for row in sub_table:
                if len(row) == 8:
                    i += 1
                    cell = row.find_all('td')

                    batter = cell[0].text.replace('Â', '')
                    batting_team = team2
                    batting_pos = i
                    runs = cell[2].text
                    balls = cell[3].text
                    fours = cell[4].text
                    sixes = cell[5].text
                    sr = cell[6].text
                    if cell[1].text == 'not out ':
                        dismissal = 'not out'
                    else:
                        dismissal = 'out'

                    # Get player link
                    player_href = cell[0].a.get('href')
                    player_link = 'https://www.espncricinfo.com' + player_href

                    # Add data to database
                    cur.execute('''INSERT OR IGNORE INTO batting_summary (match, batting_team, batter, batting_pos, runs, balls, fours, sixes, sr, dismissal, match_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (match_info, batting_team, batter, batting_pos, runs, balls, fours, sixes, sr, dismissal, match_id))

                    # Get second inning batting player info
                    get_player_info(player_link)
                    
def get_player_info(url):
    # Website
    html_text = requests.get(url).text
    time.sleep(1)

    with open('player_info.html', 'r') as html_text2:
        soup = BeautifulSoup(html_text, 'lxml')
        
        print(url)

        cover = soup.find('div', class_='ds-bg-cover ds-bg-center')
        data = soup.find('div', class_='ds-grid lg:ds-grid-cols-3 ds-grid-cols-2 ds-gap-4 ds-mb-8').find_all('div')
        
        name = cover.div.div.h1.text
        team = cover.div.div.div.span.text
        batting_style = ''
        bowling_style = ''
        education = ''

        for item in data:
            if 'Batting Style' in item.p.text:
                batting_style = item.span.text
            elif 'Bowling Style' in item.p.text:
                bowling_style = item.span.text
            elif 'Playing Role' in item.p.text:
                playing_role = item.span.text
            elif 'Education' in item.p.text:
                education = item.span.text

        # batting_style = data[3].span.h5.text
        # bowling_style = data[4].span.h5.text
        # playing_role = data[5].span.h5.text
        try:
            description = soup.find('div', class_='ci-player-bio-content').text
        except:
            description = 'None'
        image = ''

        print(f'Name: {name}')
        print(f'Team: {team}')
        print(f'Batting Style: {batting_style}')
        print(f'Bowling Style: {bowling_style}')
        print(f'Playing Role: {playing_role}')
        # print(f'Description: {description}')
        print(f'Education: {education}')

        cur.execute('''INSERT OR IGNORE INTO player_info (name, team, image, batting_style, bowling_style, playing_role, description, education, link)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)''', (name, team, image, batting_style, bowling_style, playing_role, description, education, url))


# Establish database connection
conn = sqlite3.connect('Cricket_Info.db')
cur = conn.cursor()

# Reset database
reset_database()

# Get match results
get_match_results()