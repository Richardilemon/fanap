"""
FPL Data Service - Database queries for Flask app
Extracted from app.py, no Streamlit dependencies
"""
import os
import psycopg2
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()


class FPLDataService:
    """Service class for FPL database operations"""

    CURRENT_SEASON = '2025-26'

    def __init__(self):
        self.database_url = os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in environment")

    def get_connection(self):
        """Get a fresh database connection"""
        return psycopg2.connect(self.database_url, sslmode='require')

    def execute_query(self, query, params=None):
        """Execute SQL query and return list of dicts"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return results
        finally:
            conn.close()

    def get_current_gameweek(self):
        """Get the current gameweek based on game_weeks deadline"""
        query = f"""
            SELECT gameweek
            FROM game_weeks
            WHERE season = '{self.CURRENT_SEASON}'
              AND deadline < NOW()
            ORDER BY deadline DESC
            LIMIT 1
        """
        result = self.execute_query(query)
        return result[0]['gameweek'] if result else 1

    def get_total_players(self):
        """Get total FPL managers for ownership calculations"""
        query = f"""
            SELECT total_players
            FROM game_weeks
            WHERE season = '{self.CURRENT_SEASON}'
              AND total_players IS NOT NULL
            ORDER BY gameweek DESC
            LIMIT 1
        """
        result = self.execute_query(query)
        # Fallback to 10M if not found
        return result[0]['total_players'] if result and result[0]['total_players'] else 10000000

    def get_top_scorers(self, limit=10):
        """Get top scorers this season"""
        query = f"""
            SELECT
                player_name,
                SUM(goals_scored) as total_goals,
                COUNT(DISTINCT gameweek) FILTER (WHERE minutes > 0) as games_played,
                ROUND(SUM(goals_scored)::numeric / NULLIF(COUNT(DISTINCT gameweek) FILTER (WHERE minutes > 0), 0), 2) as goals_per_game
            FROM player_gameweek_stats
            WHERE season = '{self.CURRENT_SEASON}'
            GROUP BY player_name
            HAVING SUM(goals_scored) > 0
            ORDER BY total_goals DESC
            LIMIT {limit};
        """
        return self.execute_query(query)

    def get_top_assisters(self, limit=10):
        """Get top assist providers"""
        query = f"""
            SELECT
                player_name,
                SUM(assists) as total_assists,
                COUNT(DISTINCT gameweek) FILTER (WHERE minutes > 0) as games_played,
                ROUND(SUM(assists)::numeric / NULLIF(COUNT(DISTINCT gameweek) FILTER (WHERE minutes > 0), 0), 2) as assists_per_game
            FROM player_gameweek_stats
            WHERE season = '{self.CURRENT_SEASON}'
            GROUP BY player_name
            HAVING SUM(assists) > 0
            ORDER BY total_assists DESC
            LIMIT {limit};
        """
        return self.execute_query(query)

    def get_top_points(self, limit=10):
        """Get highest points scorers"""
        query = f"""
            SELECT
                player_name,
                player_team_name as team,
                SUM(total_points) as total_points,
                SUM(minutes) as minutes,
                COUNT(DISTINCT gameweek) FILTER (WHERE minutes > 0) as games
            FROM player_gameweek_stats
            WHERE season = '{self.CURRENT_SEASON}'
            GROUP BY player_name, player_team_name
            ORDER BY total_points DESC
            LIMIT {limit};
        """
        return self.execute_query(query)

    def get_team_clean_sheets(self, limit=10):
        """Get teams with most clean sheets"""
        query = f"""
            SELECT
                player_team_name as team,
                SUM(clean_sheets) as clean_sheets,
                COUNT(DISTINCT gameweek) as games
            FROM player_gameweek_stats
            WHERE season = '{self.CURRENT_SEASON}'
              AND player_team_name IS NOT NULL
            GROUP BY player_team_name
            ORDER BY clean_sheets DESC
            LIMIT {limit};
        """
        return self.execute_query(query)

    def get_hidden_gems(self, max_ownership=5.0, limit=15):
        """Find low-owned players performing well"""
        query = f"""
            WITH total_mgrs AS (
                SELECT COALESCE(total_players, 10000000) as total
                FROM game_weeks
                WHERE season = '{self.CURRENT_SEASON}' AND total_players IS NOT NULL
                ORDER BY gameweek DESC LIMIT 1
            ),
            latest_ownership AS (
                SELECT DISTINCT ON (player_name)
                    player_name,
                    player_team_name as team,
                    ROUND(selected::numeric / (SELECT total FROM total_mgrs) * 100, 1) as ownership_pct,
                    ROUND(value / 10.0, 1) as price,
                    gameweek
                FROM player_gameweek_stats
                WHERE season = '{self.CURRENT_SEASON}'
                  AND selected > 0
                  AND player_team_name IS NOT NULL
                ORDER BY player_name, gameweek DESC
            ),
            player_stats AS (
                SELECT
                    player_name,
                    SUM(total_points) as total_points,
                    SUM(goals_scored) as goals,
                    SUM(assists) as assists,
                    ROUND(AVG(expected_goals), 2) as avg_xg,
                    ROUND(AVG(expected_assists), 2) as avg_xa
                FROM player_gameweek_stats
                WHERE season = '{self.CURRENT_SEASON}'
                  AND gameweek >= (SELECT MAX(gameweek) - 4 FROM player_gameweek_stats WHERE season = '{self.CURRENT_SEASON}')
                GROUP BY player_name
            )
            SELECT
                ps.player_name,
                lo.team,
                lo.price as current_price,
                ROUND(lo.ownership_pct, 2) as ownership,
                ps.total_points as points_last_5,
                ps.goals,
                ps.assists,
                ps.avg_xg,
                ps.avg_xa
            FROM player_stats ps
            JOIN latest_ownership lo ON ps.player_name = lo.player_name
            WHERE lo.ownership_pct < {max_ownership}
              AND ps.total_points > 20
            ORDER BY ps.total_points DESC
            LIMIT {limit};
        """
        return self.execute_query(query)

    def get_player_form(self, player_name, last_n_games=10):
        """Get player's recent form"""
        query = f"""
            SELECT
                gameweek,
                opponent_team_name as opponent,
                CASE WHEN was_home THEN 'H' ELSE 'A' END as venue,
                total_points as points,
                goals_scored as goals,
                assists,
                minutes,
                bonus,
                ROUND(expected_goals, 2) as xg,
                ROUND(expected_assists, 2) as xa
            FROM player_gameweek_stats
            WHERE player_name ILIKE %s
              AND season = '{self.CURRENT_SEASON}'
              AND minutes > 0
            ORDER BY gameweek DESC
            LIMIT {last_n_games};
        """
        return self.execute_query(query, (f'%{player_name}%',))

    def get_team_fixtures(self, team_name, next_n=5):
        """Get team's upcoming fixtures"""
        query = f"""
            WITH team_info AS (
                SELECT id, short_name
                FROM teams
                WHERE name ILIKE %s
                  AND season = '{self.CURRENT_SEASON}'
                LIMIT 1
            ),
            next_gameweeks AS (
                SELECT gameweek
                FROM game_weeks
                WHERE season = '{self.CURRENT_SEASON}'
                  AND deadline > NOW()
                ORDER BY deadline ASC
                LIMIT {next_n}
            ),
            team_fixtures AS (
                SELECT
                    f.gameweek,
                    f.team_h_code,
                    f.team_a_code,
                    f.team_h_difficulty,
                    f.team_a_difficulty,
                    ti.id as team_id
                FROM fixtures f
                CROSS JOIN team_info ti
                WHERE f.season = '{self.CURRENT_SEASON}'
                  AND f.gameweek IN (SELECT gameweek FROM next_gameweeks)
                  AND (f.team_h_code = ti.id OR f.team_a_code = ti.id)
            )
            SELECT
                tf.gameweek,
                CASE
                    WHEN tf.team_h_code = tf.team_id THEN 'H'
                    ELSE 'A'
                END as venue,
                CASE
                    WHEN tf.team_h_code = tf.team_id THEN
                        (SELECT short_name FROM teams WHERE id = tf.team_a_code AND season = '{self.CURRENT_SEASON}')
                    ELSE
                        (SELECT short_name FROM teams WHERE id = tf.team_h_code AND season = '{self.CURRENT_SEASON}')
                END as opponent,
                CASE
                    WHEN tf.team_h_code = tf.team_id THEN tf.team_h_difficulty
                    ELSE tf.team_a_difficulty
                END as difficulty
            FROM team_fixtures tf
            ORDER BY tf.gameweek;
        """
        return self.execute_query(query, (f'%{team_name}%',))

    def get_fixture_difficulty_matrix(self, next_n=5):
        """Get fixture difficulty for all teams for next N gameweeks"""
        current_gw = self.get_current_gameweek()

        query = f"""
            WITH next_gameweeks AS (
                SELECT gameweek
                FROM game_weeks
                WHERE season = '{self.CURRENT_SEASON}'
                  AND deadline > NOW()
                ORDER BY deadline ASC
                LIMIT {next_n}
            ),
            all_teams AS (
                SELECT id, short_name, name
                FROM teams
                WHERE season = '{self.CURRENT_SEASON}'
                ORDER BY name
            ),
            team_fixtures AS (
                SELECT
                    t.short_name as team,
                    t.name as team_name,
                    f.gameweek,
                    CASE
                        WHEN f.team_h_code = t.id THEN 'H'
                        ELSE 'A'
                    END as venue,
                    CASE
                        WHEN f.team_h_code = t.id THEN
                            (SELECT short_name FROM teams WHERE id = f.team_a_code AND season = '{self.CURRENT_SEASON}')
                        ELSE
                            (SELECT short_name FROM teams WHERE id = f.team_h_code AND season = '{self.CURRENT_SEASON}')
                    END as opponent,
                    CASE
                        WHEN f.team_h_code = t.id THEN f.team_h_difficulty
                        ELSE f.team_a_difficulty
                    END as difficulty
                FROM all_teams t
                JOIN fixtures f ON (f.team_h_code = t.id OR f.team_a_code = t.id)
                WHERE f.season = '{self.CURRENT_SEASON}'
                  AND f.gameweek IN (SELECT gameweek FROM next_gameweeks)
            )
            SELECT team, team_name, gameweek, venue, opponent, difficulty
            FROM team_fixtures
            ORDER BY team_name, gameweek;
        """

        results = self.execute_query(query)

        # Transform into matrix format: {team: [{gw, venue, opponent, difficulty}, ...]}
        matrix = {}
        gameweeks = set()

        for row in results:
            team = row['team']
            if team not in matrix:
                matrix[team] = {
                    'name': row['team_name'],
                    'short_name': team,
                    'fixtures': []
                }
            matrix[team]['fixtures'].append({
                'gameweek': row['gameweek'],
                'venue': row['venue'],
                'opponent': row['opponent'],
                'difficulty': row['difficulty']
            })
            gameweeks.add(row['gameweek'])

        return {
            'teams': list(matrix.values()),
            'gameweeks': sorted(list(gameweeks))
        }

    def get_upcoming_fixtures(self, gameweek=None):
        """Get fixtures for a specific gameweek or next upcoming"""
        if gameweek is None:
            gameweek = self.get_current_gameweek() + 1

        query = f"""
            SELECT
                f.gameweek,
                f.kickoff_time,
                th.short_name as home_team,
                th.name as home_team_name,
                ta.short_name as away_team,
                ta.name as away_team_name,
                f.team_h_score as home_score,
                f.team_a_score as away_score,
                f.team_h_difficulty as home_difficulty,
                f.team_a_difficulty as away_difficulty
            FROM fixtures f
            JOIN teams th ON f.team_h_code = th.id AND th.season = '{self.CURRENT_SEASON}'
            JOIN teams ta ON f.team_a_code = ta.id AND ta.season = '{self.CURRENT_SEASON}'
            WHERE f.season = '{self.CURRENT_SEASON}'
              AND f.gameweek = %s
            ORDER BY f.kickoff_time;
        """
        return self.execute_query(query, (gameweek,))

    def get_all_teams(self):
        """Get all teams for the current season"""
        query = f"""
            SELECT id, code, name, short_name
            FROM teams
            WHERE season = '{self.CURRENT_SEASON}'
            ORDER BY name;
        """
        return self.execute_query(query)

    def compare_players(self, player1, player2):
        """Compare two players' stats"""
        query = f"""
            WITH total_mgrs AS (
                SELECT COALESCE(total_players, 10000000) as total
                FROM game_weeks
                WHERE season = '{self.CURRENT_SEASON}' AND total_players IS NOT NULL
                ORDER BY gameweek DESC LIMIT 1
            )
            SELECT
                player_name,
                player_team_name as team,
                SUM(total_points) as points,
                SUM(goals_scored) as goals,
                SUM(assists) as assists,
                SUM(minutes) as minutes,
                SUM(bonus) as bonus,
                ROUND(AVG(expected_goals), 2) as avg_xg,
                ROUND(AVG(expected_assists), 2) as avg_xa,
                ROUND(AVG(expected_goals) + AVG(expected_assists), 2) as avg_xgi,
                COUNT(DISTINCT gameweek) FILTER (WHERE minutes > 0) as games_played,
                ROUND(SUM(total_points)::numeric / NULLIF(COUNT(DISTINCT gameweek) FILTER (WHERE minutes > 0), 0), 1) as points_per_game,
                ROUND(MAX(value) / 10.0, 1) as current_price,
                ROUND(MAX(selected)::numeric / (SELECT total FROM total_mgrs) * 100, 1) as ownership
            FROM player_gameweek_stats
            WHERE season = '{self.CURRENT_SEASON}'
              AND (player_name ILIKE %s OR player_name ILIKE %s)
            GROUP BY player_name, player_team_name
            ORDER BY
                CASE
                    WHEN player_name ILIKE %s THEN 1
                    ELSE 2
                END;
        """
        return self.execute_query(query, (f'%{player1}%', f'%{player2}%', f'%{player1}%'))

    def get_player_stats(self, player_name):
        """Get detailed stats for a single player"""
        query = f"""
            WITH total_mgrs AS (
                SELECT COALESCE(total_players, 10000000) as total
                FROM game_weeks
                WHERE season = '{self.CURRENT_SEASON}' AND total_players IS NOT NULL
                ORDER BY gameweek DESC LIMIT 1
            )
            SELECT
                pgs.player_name,
                pgs.player_team_name as team,
                p.position,
                SUM(pgs.total_points) as points,
                SUM(pgs.goals_scored) as goals,
                SUM(pgs.assists) as assists,
                SUM(pgs.minutes) as minutes,
                SUM(pgs.bonus) as bonus,
                SUM(pgs.clean_sheets) as clean_sheets,
                ROUND(AVG(pgs.expected_goals), 2) as avg_xg,
                ROUND(AVG(pgs.expected_assists), 2) as avg_xa,
                ROUND(AVG(pgs.ict_index), 1) as avg_ict,
                ROUND(AVG(pgs.influence), 1) as avg_influence,
                ROUND(AVG(pgs.creativity), 1) as avg_creativity,
                ROUND(AVG(pgs.threat), 1) as avg_threat,
                COUNT(DISTINCT pgs.gameweek) FILTER (WHERE pgs.minutes > 0) as games_played,
                ROUND(SUM(pgs.total_points)::numeric / NULLIF(COUNT(DISTINCT pgs.gameweek) FILTER (WHERE pgs.minutes > 0), 0), 1) as points_per_game,
                ROUND(MAX(pgs.value) / 10.0, 1) as current_price,
                ROUND(MAX(pgs.selected)::numeric / (SELECT total FROM total_mgrs) * 100, 1) as ownership
            FROM player_gameweek_stats pgs
            LEFT JOIN players p ON (p.first_name || ' ' || p.second_name) = pgs.player_name
                                OR p.web_name = pgs.player_name
            WHERE pgs.player_name ILIKE %s
              AND pgs.season = '{self.CURRENT_SEASON}'
            GROUP BY pgs.player_name, pgs.player_team_name, p.position
            LIMIT 1;
        """
        result = self.execute_query(query, (f'%{player_name}%',))
        return result[0] if result else None

    def get_player_fixtures(self, player_name, next_n=5):
        """Get player's upcoming fixtures"""
        query = f"""
            WITH player_team_info AS (
                SELECT t.id as team_id, t.name as team_name, t.short_name
                FROM players p
                JOIN teams t ON p.team_code = t.code AND t.season = '{self.CURRENT_SEASON}'
                WHERE (p.first_name || ' ' || p.second_name) ILIKE %s
                   OR p.web_name ILIKE %s
                LIMIT 1
            ),
            next_gameweeks AS (
                SELECT gameweek
                FROM game_weeks
                WHERE season = '{self.CURRENT_SEASON}'
                  AND deadline > NOW()
                ORDER BY deadline ASC
                LIMIT {next_n}
            ),
            player_fixtures AS (
                SELECT
                    f.gameweek,
                    f.team_h_code,
                    f.team_a_code,
                    f.team_h_difficulty,
                    f.team_a_difficulty,
                    pti.team_id as player_team_id
                FROM fixtures f
                CROSS JOIN player_team_info pti
                WHERE f.season = '{self.CURRENT_SEASON}'
                  AND f.gameweek IN (SELECT gameweek FROM next_gameweeks)
                  AND (f.team_h_code = pti.team_id OR f.team_a_code = pti.team_id)
            )
            SELECT
                pf.gameweek,
                CASE
                    WHEN pf.team_h_code = pf.player_team_id THEN 'H'
                    ELSE 'A'
                END as venue,
                CASE
                    WHEN pf.team_h_code = pf.player_team_id THEN
                        (SELECT short_name FROM teams WHERE id = pf.team_a_code AND season = '{self.CURRENT_SEASON}')
                    ELSE
                        (SELECT short_name FROM teams WHERE id = pf.team_h_code AND season = '{self.CURRENT_SEASON}')
                END as opponent,
                CASE
                    WHEN pf.team_h_code = pf.player_team_id THEN pf.team_h_difficulty
                    ELSE pf.team_a_difficulty
                END as difficulty
            FROM player_fixtures pf
            ORDER BY pf.gameweek;
        """
        return self.execute_query(query, (f'%{player_name}%', f'%{player_name}%'))


# Singleton instance for convenience
_service = None

def get_fpl_service():
    """Get singleton FPL data service instance"""
    global _service
    if _service is None:
        _service = FPLDataService()
    return _service
