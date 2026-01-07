"""
Fixtures routes
"""
from flask import Blueprint, render_template, request
from services.fpl_data import get_fpl_service

fixtures_bp = Blueprint('fixtures', __name__)


@fixtures_bp.route('/fixtures')
def fixtures():
    """Fixtures page - FDR matrix and upcoming matches"""
    service = get_fpl_service()

    current_gw = service.get_current_gameweek()

    # Get gameweek from query param or use next gameweek
    selected_gw = request.args.get('gw', type=int, default=current_gw + 1)

    # Get fixture difficulty matrix (next 5 gameweeks)
    fdr_matrix = service.get_fixture_difficulty_matrix(5)

    # Get upcoming fixtures for selected gameweek
    upcoming = service.get_upcoming_fixtures(selected_gw)

    # Get all teams for filter dropdown
    all_teams = service.get_all_teams()

    return render_template(
        'fixtures.html',
        gameweek=current_gw,
        selected_gw=selected_gw,
        fdr_matrix=fdr_matrix,
        upcoming_fixtures=upcoming,
        teams=all_teams,
        active_page='fixtures'
    )
