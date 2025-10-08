 INSERT INTO teams (
                    season, code, id, name, short_name,
                    strength_overall_home, strength_overall_away,
                    strength_attack_home, strength_attack_away,
                    strength_defence_home, strength_defence_away,
                    strength
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (season, code) DO UPDATE SET
                    id = EXCLUDED.id,
                    name = EXCLUDED.name,
                    short_name = EXCLUDED.short_name,
                    strength_overall_home = EXCLUDED.strength_overall_home,
                    strength_overall_away = EXCLUDED.strength_overall_away,
                    strength_attack_home = EXCLUDED.strength_attack_home,
                    strength_attack_away = EXCLUDED.strength_attack_away,
                    strength_defence_home = EXCLUDED.strength_defence_home,
                    strength_defence_away = EXCLUDED.strength_defence_away,
                    strength = EXCLUDED.strength;