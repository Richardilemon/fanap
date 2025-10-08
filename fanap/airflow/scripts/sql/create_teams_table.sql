CREATE TABLE
    IF NOT EXISTS teams (
        season TEXT,
        code INTEGER,
        id INTEGER,
        name TEXT,
        short_name TEXT,
        strength_overall_home INTEGER,
        strength_overall_away INTEGER,
        strength_attack_home INTEGER,
        strength_attack_away INTEGER,
        strength_defence_home INTEGER,
        strength_defence_away INTEGER,
        strength INTEGER,
        PRIMARY KEY (season, code)
    );