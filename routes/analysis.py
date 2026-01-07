"""
Analysis routes - Discovery, Comparison, Player Research
"""
from flask import Blueprint, render_template, request
from services.fpl_data import get_fpl_service

analysis_bp = Blueprint('analysis', __name__, url_prefix='/analysis')


@analysis_bp.route('/')
def analysis_index():
    """Redirect to discovery page"""
    return render_template('analysis1.html', active_page='analysis', active_sub='discovery')


@analysis_bp.route('/discovery')
def discovery():
    """Hidden Gems / Discovery page"""
    service = get_fpl_service()

    # Get filter parameters
    max_ownership = request.args.get('max_own', type=float, default=5.0)

    # Get hidden gems
    gems = service.get_hidden_gems(max_ownership=max_ownership, limit=15)

    # Get fixtures for each gem
    for gem in gems:
        fixtures = service.get_team_fixtures(gem['team'], next_n=3)
        gem['fixtures'] = fixtures if fixtures else []

    # Calculate summary stats
    summary = {
        'count': len(gems),
        'avg_price': round(sum(g['current_price'] for g in gems) / len(gems), 1) if gems else 0,
        'avg_points': round(sum(g['points_last_5'] for g in gems) / len(gems), 1) if gems else 0
    }

    return render_template(
        'analysis1.html',
        gems=gems,
        summary=summary,
        max_ownership=max_ownership,
        active_page='analysis',
        active_sub='discovery'
    )


@analysis_bp.route('/compare')
def compare():
    """Player Comparison page"""
    service = get_fpl_service()

    # Get player names from query params
    player1_name = request.args.get('p1', default='')
    player2_name = request.args.get('p2', default='')

    comparison = None
    player1_fixtures = []
    player2_fixtures = []

    if player1_name and player2_name:
        # Get comparison data
        comparison = service.compare_players(player1_name, player2_name)

        if comparison and len(comparison) >= 1:
            # Get fixtures for player 1
            player1_fixtures = service.get_player_fixtures(player1_name, next_n=5)

        if comparison and len(comparison) >= 2:
            # Get fixtures for player 2
            player2_fixtures = service.get_player_fixtures(player2_name, next_n=5)

    return render_template(
        'analysis2.html',
        player1_name=player1_name,
        player2_name=player2_name,
        comparison=comparison,
        player1_fixtures=player1_fixtures or [],
        player2_fixtures=player2_fixtures or [],
        active_page='analysis',
        active_sub='compare'
    )


@analysis_bp.route('/player')
def player_research():
    """Player Research / Deep Dive page"""
    service = get_fpl_service()

    player_name = request.args.get('name', default='')

    player_data = None
    player_form = []
    player_fixtures = []

    if player_name:
        # Get player stats
        player_data = service.get_player_stats(player_name)

        # Get recent form
        player_form = service.get_player_form(player_name, last_n_games=10)

        # Get upcoming fixtures
        player_fixtures = service.get_player_fixtures(player_name, next_n=5)

    return render_template(
        'analysis3.html',
        player_name=player_name,
        player=player_data,
        form=player_form or [],
        fixtures=player_fixtures or [],
        active_page='analysis',
        active_sub='player'
    )
