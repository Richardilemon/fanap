"""
League Leaders routes
"""
from flask import Blueprint, render_template
from services.fpl_data import get_fpl_service

league_bp = Blueprint('league', __name__)


@league_bp.route('/league-leaders')
def league_leaders():
    """League Leaders page - top scorers, assisters, points, defense"""
    service = get_fpl_service()

    # Get data for all sections
    top_scorers = service.get_top_scorers(10)
    top_assisters = service.get_top_assisters(10)
    top_points = service.get_top_points(10)
    defense_stats = service.get_team_clean_sheets(10)
    current_gw = service.get_current_gameweek()

    # Calculate bar percentages for charts
    if top_scorers:
        max_goals = max(p['total_goals'] for p in top_scorers)
        for player in top_scorers:
            player['bar_pct'] = (player['total_goals'] / max_goals * 100) if max_goals > 0 else 0

    if top_assisters:
        max_assists = max(p['total_assists'] for p in top_assisters)
        for player in top_assisters:
            player['bar_pct'] = (player['total_assists'] / max_assists * 100) if max_assists > 0 else 0

    # Best defense percentage
    best_defense = None
    if defense_stats:
        best_defense = defense_stats[0]
        total_cs = sum(t['clean_sheets'] for t in defense_stats)
        best_defense['percentage'] = int((best_defense['clean_sheets'] / total_cs * 100)) if total_cs > 0 else 0

    return render_template(
        'league_leaders.html',
        gameweek=current_gw,
        top_scorers=top_scorers[:3],  # Top 3 for cards
        top_scorers_full=top_scorers,  # Full list for charts
        top_assisters=top_assisters[:3],
        top_assisters_full=top_assisters,
        top_points=top_points[:3],
        defense_stats=defense_stats[:5],
        best_defense=best_defense,
        active_page='league_leaders'
    )
