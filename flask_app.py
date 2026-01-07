"""
Fanap - Flask Application
FPL Analytics Dashboard
"""
import os
from flask import Flask, render_template, redirect, url_for
from dotenv import load_dotenv

load_dotenv()

# Create Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key-change-in-prod')

# Register blueprints
from routes.league import league_bp
from routes.fixtures import fixtures_bp
from routes.analysis import analysis_bp

app.register_blueprint(league_bp)
app.register_blueprint(fixtures_bp)
app.register_blueprint(analysis_bp)


@app.route('/')
def index():
    """Home page - redirect to dashboard"""
    return redirect(url_for('league.league_leaders'))


@app.route('/dashboard')
def dashboard():
    """Main dashboard page (placeholder for now)"""
    return render_template('dashboard.html')


@app.context_processor
def inject_globals():
    """Inject global variables into all templates"""
    from services.fpl_data import get_fpl_service
    service = get_fpl_service()
    return {
        'current_gameweek': service.get_current_gameweek(),
        'season': service.CURRENT_SEASON
    }


# FDR color helper for templates
@app.template_filter('fdr_color')
def fdr_color_filter(difficulty):
    """Return Tailwind CSS classes for fixture difficulty"""
    colors = {
        1: 'bg-fdr-2 text-black',  # Very easy - green
        2: 'bg-fdr-2 text-black',  # Easy - green
        3: 'bg-fdr-3 text-black',  # Medium - yellow/gray
        4: 'bg-fdr-4 text-white',  # Hard - red
        5: 'bg-fdr-5 text-white',  # Very hard - dark red
    }
    return colors.get(difficulty, 'bg-gray-500 text-white')


@app.template_filter('fdr_border')
def fdr_border_filter(difficulty):
    """Return border color for fixture difficulty"""
    colors = {
        1: 'border-l-[#0bda50]',  # Green
        2: 'border-l-[#0bda50]',  # Green
        3: 'border-l-[#9cbaba]',  # Gray
        4: 'border-l-[#ef4444]',  # Red
        5: 'border-l-[#ef4444]',  # Red
    }
    return colors.get(difficulty, 'border-l-gray-500')


if __name__ == '__main__':
    app.run(debug=True, port=5001)
