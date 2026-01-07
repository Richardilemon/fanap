"""
Estimate Penalty Goals from Available Data
Uses multiple heuristics to make educated guesses
"""
from dotenv import load_dotenv
from scripts.utils.db_config import get_db_connection

load_dotenv()


def estimate_penalty_goals(season='2025-26', dry_run=True):
    """
    Estimate penalties scored using available data.
    
    Heuristics:
    1. If player missed a penalty but scored in same game -> likely scored other penalty
    2. If player is team's penalty taker and team won a penalty (opponent saved) -> likely scored
    3. If player scored 1 goal in a game and no other forwards/mids scored -> possibly penalty
    
    Args:
        season: Season to analyze
        dry_run: If True, only show estimates without updating DB
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print(f"🧮 ESTIMATING PENALTY GOALS FOR {season}")
    print("=" * 80)
    print("\n⚠️  Important: These are ESTIMATES based on available data")
    print("   FPL API does not track penalties scored separately\n")
    
    estimates = []
    
    # ============================================================================
    # HEURISTIC 1: Player missed penalty but team scored penalty in same game
    # ============================================================================
    print("🔍 Heuristic 1: Checking games with multiple penalty opportunities...\n")
    
    cursor.execute("""
        WITH penalty_games AS (
            SELECT 
                season,
                gameweek,
                fixture_id,
                SUM(penalties_missed) as team_pens_missed,
                SUM(penalties_saved) as opponent_saved
            FROM player_gameweek_stats
            WHERE season = %s
            GROUP BY season, gameweek, fixture_id
            HAVING SUM(penalties_missed) > 0 OR SUM(penalties_saved) > 0
        )
        SELECT 
            pgs.season,
            pgs.gameweek,
            pgs.player_name,
            pgs.fixture_id,
            pgs.goals_scored,
            pgs.penalties_missed,
            pg.team_pens_missed,
            pg.opponent_saved
        FROM player_gameweek_stats pgs
        JOIN penalty_games pg 
            ON pgs.season = pg.season 
            AND pgs.gameweek = pg.gameweek 
            AND pgs.fixture_id = pg.fixture_id
        WHERE pgs.season = %s
          AND pgs.goals_scored > 0
          AND pgs.penalties_scored = 0  -- Not already set
        ORDER BY pgs.gameweek, pgs.player_name;
    """, (season, season))
    
    for row in cursor.fetchall():
        season_val, gw, player, fixture, goals, missed, team_missed, opp_saved = row
        
        # If opponent saved a penalty, someone likely scored it
        if opp_saved > 0:
            estimates.append({
                'season': season_val,
                'gameweek': gw,
                'player': player,
                'fixture': fixture,
                'estimated_penalties': min(1, goals),  # Assume at most 1 penalty per game
                'confidence': 'medium',
                'reason': f'Opponent saved {opp_saved} penalty(s), player scored {goals} goal(s)'
            })
    
    # ============================================================================
    # HEURISTIC 2: Known penalty taker scored in a low-scoring game
    # ============================================================================
    print("🔍 Heuristic 2: Checking penalty takers in low-scoring games...\n")
    
    cursor.execute("""
        SELECT 
            pgs.season,
            pgs.gameweek,
            pgs.player_name,
            pgs.fixture_id,
            pgs.goals_scored,
            pt.confidence as pk_confidence,
            pt.priority
        FROM player_gameweek_stats pgs
        JOIN penalty_takers pt 
            ON pgs.player_name = pt.player_name 
            AND pgs.season = pt.season
        WHERE pgs.season = %s
          AND pgs.goals_scored = 1  -- Scored exactly 1 goal
          AND pgs.penalties_missed = 0  -- Didn't miss
          AND pgs.penalties_scored = 0  -- Not already set
          AND pt.priority = 1  -- Primary penalty taker
        ORDER BY pgs.gameweek;
    """, (season,))
    
    for row in cursor.fetchall():
        season_val, gw, player, fixture, goals, pk_conf, priority = row
        
        # Check if this was a 1-0 or 1-1 game (more likely to be penalty)
        cursor.execute("""
            SELECT team_h_score, team_a_score
            FROM fixtures
            WHERE code = %s;
        """, (fixture,))
        
        score = cursor.fetchone()
        if score:
            h_score, a_score = score
            total_goals = (h_score or 0) + (a_score or 0)
            
            if total_goals <= 2:  # Low-scoring game
                estimates.append({
                    'season': season_val,
                    'gameweek': gw,
                    'player': player,
                    'fixture': fixture,
                    'estimated_penalties': 1,
                    'confidence': 'low',
                    'reason': f'Primary pen taker scored in low-scoring game ({h_score}-{a_score})'
                })
    
    # ============================================================================
    # Display Estimates
    # ============================================================================
    if not estimates:
        print("✅ No penalty goals to estimate (or all already recorded)\n")
        cursor.close()
        conn.close()
        return
    
    print(f"📊 Found {len(estimates)} potential penalty goals:\n")
    
    for est in estimates:
        conf_emoji = "🟢" if est['confidence'] == 'high' else "🟡" if est['confidence'] == 'medium' else "🟠"
        print(f"{conf_emoji} GW{est['gameweek']}: {est['player']}")
        print(f"   Estimated penalties: {est['estimated_penalties']}")
        print(f"   Confidence: {est['confidence']}")
        print(f"   Reason: {est['reason']}")
        print()
    
    # ============================================================================
    # Apply Updates (if not dry run)
    # ============================================================================
    if dry_run:
        print("=" * 80)
        print("🔍 DRY RUN - No changes made to database")
        print("   Run with --apply flag to save estimates")
        print("=" * 80)
    else:
        print("=" * 80)
        apply = input("Apply these estimates to database? (yes/no): ").strip().lower()
        
        if apply == 'yes':
            updated = 0
            for est in estimates:
                cursor.execute("""
                    UPDATE player_gameweek_stats
                    SET 
                        penalties_scored = %s,
                        penalties_scored_verified = FALSE
                    WHERE season = %s 
                      AND gameweek = %s 
                      AND player_name = %s 
                      AND fixture_id = %s;
                """, (
                    est['estimated_penalties'],
                    est['season'],
                    est['gameweek'],
                    est['player'],
                    est['fixture']
                ))
                updated += cursor.rowcount
            
            conn.commit()
            print(f"\n✅ Updated {updated} records with penalty estimates")
            print("⚠️  Remember: These are estimates! Verify with match reports")
        else:
            print("\n❌ Cancelled - no changes made")
    
    cursor.close()
    conn.close()


def manually_verify_penalty(season, gameweek, player_name, penalties_scored):
    """
    Manually set a verified penalty goal count for a specific game.
    Use this when you've confirmed from match reports.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE player_gameweek_stats
        SET 
            penalties_scored = %s,
            penalties_scored_verified = TRUE
        WHERE season = %s 
          AND gameweek = %s 
          AND player_name = %s
        RETURNING fixture_id;
    """, (penalties_scored, season, gameweek, player_name))
    
    result = cursor.fetchone()
    
    if result:
        conn.commit()
        print(f"✅ Verified: {player_name} scored {penalties_scored} penalty(s) in GW{gameweek}")
    else:
        print(f"❌ No matching record found for {player_name} in GW{gameweek}")
    
    cursor.close()
    conn.close()


def show_penalty_stats(season='2025-26'):
    """Show summary of penalty statistics"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print(f"📊 PENALTY STATISTICS - {season}")
    print("=" * 80)
    
    # Overall stats
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT player_name) as players,
            SUM(penalties_scored) as total_scored,
            SUM(penalties_missed) as total_missed,
            SUM(CASE WHEN penalties_scored_verified THEN penalties_scored ELSE 0 END) as verified_scored,
            SUM(CASE WHEN penalties_scored > 0 AND NOT penalties_scored_verified THEN 1 ELSE 0 END) as estimated_count
        FROM player_gameweek_stats
        WHERE season = %s
          AND (penalties_scored > 0 OR penalties_missed > 0);
    """, (season,))
    
    stats = cursor.fetchone()
    if stats:
        players, scored, missed, verified, estimated = stats
        total = (scored or 0) + (missed or 0)
        conversion = (scored / total * 100) if total > 0 else 0
        
        print(f"\n📈 Overall:")
        print(f"   Players who've taken penalties: {players}")
        print(f"   Total penalties: {total}")
        print(f"   Scored: {scored} ({conversion:.1f}%)")
        print(f"   Missed: {missed}")
        print(f"   Verified: {verified}")
        print(f"   Estimated: {estimated}")
    
    # Top penalty takers
    print(f"\n🎯 Top Penalty Takers:")
    cursor.execute("""
        SELECT 
            player_name,
            SUM(penalties_scored) as scored,
            SUM(penalties_missed) as missed,
            SUM(penalties_scored + penalties_missed) as total,
            ROUND(
                CAST(SUM(penalties_scored) AS DECIMAL) / 
                NULLIF(SUM(penalties_scored + penalties_missed), 0) * 100, 
                1
            ) as conversion_rate,
            SUM(CASE WHEN penalties_scored_verified THEN 1 ELSE 0 END) as verified_games
        FROM player_gameweek_stats
        WHERE season = %s
          AND (penalties_scored > 0 OR penalties_missed > 0)
        GROUP BY player_name
        ORDER BY total DESC, conversion_rate DESC
        LIMIT 10;
    """, (season,))
    
    print(f"{'Player':<25} {'Scored':<8} {'Missed':<8} {'Total':<8} {'Conv%':<8} {'Verified'}")
    print("-" * 80)
    
    for row in cursor.fetchall():
        player, scored, missed, total, conv, verified = row
        verified_flag = f"✓ {verified}" if verified > 0 else "✗"
        print(f"{player:<25} {scored:<8} {missed:<8} {total:<8} {conv or 0:<8.1f} {verified_flag}")
    
    print("\n" + "=" * 80)
    
    cursor.close()
    conn.close()


def clear_estimates(season='2025-26'):
    """Clear all estimated (non-verified) penalty data"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE player_gameweek_stats
        SET penalties_scored = 0
        WHERE season = %s 
          AND penalties_scored_verified = FALSE;
    """, (season,))
    
    cleared = cursor.rowcount
    conn.commit()
    
    print(f"✅ Cleared {cleared} estimated penalty records for {season}")
    
    cursor.close()
    conn.close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Estimate and manage penalty goals')
    parser.add_argument('--season', default='2025-26', help='Season to analyze')
    parser.add_argument('--apply', action='store_true', help='Apply estimates to database')
    parser.add_argument('--stats', action='store_true', help='Show penalty statistics')
    parser.add_argument('--verify', nargs=4, metavar=('SEASON', 'GW', 'PLAYER', 'COUNT'),
                       help='Manually verify: --verify 2025-26 10 "Erling Haaland" 2')
    parser.add_argument('--clear', action='store_true', help='Clear all estimates (keep verified only)')
    
    args = parser.parse_args()
    
    if args.stats:
        show_penalty_stats(args.season)
    elif args.verify:
        season, gw, player, count = args.verify
        manually_verify_penalty(season, int(gw), player, int(count))
    elif args.clear:
        confirm = input(f"Clear all estimated penalties for {args.season}? (yes/no): ").strip().lower()
        if confirm == 'yes':
            clear_estimates(args.season)
    else:
        estimate_penalty_goals(args.season, dry_run=not args.apply)


if __name__ == '__main__':
    main()