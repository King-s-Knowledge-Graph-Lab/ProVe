import json
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from datetime import datetime


# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

def load_info_json(file_path='info.json'):
    """Load entire JSON file into memory (acceptable at 784KB)"""
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data


def process_request_metrics(data):
    """Extract request_type data into DataFrame with count, min/max/avg times"""
    request_data = []
    for endpoint, metrics in data['request_type'].items():
        request_data.append({
            'endpoint': endpoint,
            'count': metrics['count'],
            'min_time': metrics['min_execution_time'],
            'max_time': metrics['max_execution_time'],
            'avg_time': metrics['average_execution_time']
        })
    df = pd.DataFrame(request_data)
    df = df.sort_values('count', ascending=False)
    return df


def process_geographic_data(data):
    """Process country_code data for choropleth map"""
    geo_data = []
    country_codes = data.get('country_code', {})
    country_names = data.get('country_name', {})

    for code, count in country_codes.items():
        if code != 'Not found' and code != '':
            geo_data.append({
                'country_code': code,
                'count': count
            })

    df = pd.DataFrame(geo_data)
    df = df.sort_values('count', ascending=False)
    return df


def process_monthly_usage(data):
    """Convert month_year to time series, sort chronologically"""
    monthly_data = []
    for month_year, count in data['month_year'].items():
        # Parse "03-2025" format
        month, year = month_year.split('-')
        date_str = f"{year}-{month}-01"
        monthly_data.append({
            'date': datetime.strptime(date_str, '%Y-%m-%d'),
            'month_year': f"{year}-{month}",
            'count': count
        })

    df = pd.DataFrame(monthly_data)
    df = df.sort_values('date')
    return df


def calculate_kpi_metrics(data):
    """Calculate: total_requests, unique_users, countries_count, avg_execution_time"""
    total_requests = sum(metrics['count'] for metrics in data['request_type'].values())
    unique_users = len(data['hash'])

    # Count non-empty country codes
    countries_count = len([code for code, count in data['country_code'].items()
                          if code != 'Not found' and code != ''])

    # Calculate weighted average execution time
    total_time = 0
    total_count = 0
    for metrics in data['request_type'].values():
        total_time += metrics['average_execution_time'] * metrics['count']
        total_count += metrics['count']
    avg_execution_time = total_time / total_count if total_count > 0 else 0

    return {
        'total_requests': total_requests,
        'unique_users': unique_users,
        'countries_count': countries_count,
        'avg_execution_time': avg_execution_time
    }


# ============================================================================
# VISUALIZATION BUILDER FUNCTIONS
# ============================================================================

def create_kpi_card(value, label, color="primary"):
    """Create a KPI card with value and label"""
    card = dbc.Card([
        dbc.CardBody([
            html.H2(f"{value:,}" if isinstance(value, int) else f"{value:.2f}s",
                   className="card-title text-center mb-0",
                   style={'fontSize': '2.5rem', 'fontWeight': 'bold'}),
            html.P(label, className="card-text text-center text-muted")
        ])
    ], color=color, outline=True, className="mb-3")
    return card


def create_monthly_trend_chart(df_monthly):
    """Create monthly usage trend line chart"""
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_monthly['date'],
        y=df_monthly['count'],
        mode='lines+markers',
        name='Requests',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=8),
        hovertemplate='<b>%{x|%Y-%m}</b><br>Requests: %{y:,}<extra></extra>'
    ))

    fig.update_layout(
        title='Monthly Usage Trend',
        xaxis_title='Month',
        yaxis_title='Number of Requests',
        hovermode='x unified',
        template='plotly_white',
        height=400
    )

    return fig


def create_request_performance_chart(df_requests):
    """Create request type performance bar chart"""
    fig = go.Figure()

    # Create bar chart with color based on average execution time
    fig.add_trace(go.Bar(
        x=df_requests['endpoint'],
        y=df_requests['count'],
        marker=dict(
            color=df_requests['avg_time'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title='Avg Time (s)')
        ),
        hovertemplate='<b>%{x}</b><br>Count: %{y:,}<br>Avg Time: %{marker.color:.2f}s<extra></extra>'
    ))

    fig.update_layout(
        title='Request Type Performance',
        xaxis_title='Endpoint',
        yaxis_title='Request Count',
        template='plotly_white',
        height=500,
        xaxis={'tickangle': -45}
    )

    return fig


def create_geo_choropleth(df_geo):
    """Create geographic choropleth world map"""
    fig = go.Figure()

    fig.add_trace(go.Choropleth(
        locations=df_geo['country_code'],
        z=df_geo['count'],
        locationmode='ISO-3',
        colorscale='Blues',
        colorbar_title='Requests',
        hovertemplate='<b>%{location}</b><br>Requests: %{z:,}<extra></extra>'
    ))

    fig.update_layout(
        title='Geographic Distribution of Requests',
        geo=dict(
            showframe=False,
            showcoastlines=True,
            projection_type='equirectangular'
        ),
        height=600,
        template='plotly_white'
    )

    return fig


def create_top_countries_chart(df_geo, top_n=10):
    """Create top countries bar chart"""
    df_top = df_geo.head(top_n)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df_top['count'],
        y=df_top['country_code'],
        orientation='h',
        marker=dict(color='#2ca02c'),
        hovertemplate='<b>%{y}</b><br>Requests: %{x:,}<extra></extra>'
    ))

    fig.update_layout(
        title=f'Top {top_n} Countries by Request Count',
        xaxis_title='Number of Requests',
        yaxis_title='Country Code',
        template='plotly_white',
        height=400,
        yaxis={'categoryorder': 'total ascending'}
    )

    return fig


def create_top_cities_chart(data, top_n=10):
    """Create top cities bar chart"""
    cities = data.get('city', {})
    city_data = [{'city': city, 'count': count} for city, count in cities.items()
                 if city != 'Not found' and city != '']
    df_cities = pd.DataFrame(city_data).sort_values('count', ascending=False).head(top_n)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df_cities['count'],
        y=df_cities['city'],
        orientation='h',
        marker=dict(color='#ff7f0e'),
        hovertemplate='<b>%{y}</b><br>Requests: %{x:,}<extra></extra>'
    ))

    fig.update_layout(
        title=f'Top {top_n} Cities by Request Count',
        xaxis_title='Number of Requests',
        yaxis_title='City',
        template='plotly_white',
        height=400,
        yaxis={'categoryorder': 'total ascending'}
    )

    return fig


def create_execution_time_boxplot(df_requests):
    """Create execution time distribution box plot"""
    fig = go.Figure()

    for _, row in df_requests.iterrows():
        fig.add_trace(go.Box(
            y=[row['min_time'], row['avg_time'], row['max_time']],
            name=row['endpoint'],
            boxmean='sd'
        ))

    fig.update_layout(
        title='Execution Time Distribution by Request Type',
        yaxis_title='Execution Time (seconds)',
        template='plotly_white',
        height=500,
        xaxis={'tickangle': -45},
        showlegend=False
    )

    return fig


def create_request_distribution_pie(df_requests):
    """Create request distribution pie chart"""
    fig = go.Figure()

    fig.add_trace(go.Pie(
        labels=df_requests['endpoint'],
        values=df_requests['count'],
        hovertemplate='<b>%{label}</b><br>Count: %{value:,}<br>Percentage: %{percent}<extra></extra>'
    ))

    fig.update_layout(
        title='Request Distribution by Endpoint',
        template='plotly_white',
        height=400
    )

    return fig


def create_top_qids_chart(data, top_n=15):
    """Create top Wikidata QIDs bar chart"""
    qids = data.get('qid', {})
    qid_data = [{'qid': qid, 'count': count} for qid, count in qids.items()]
    df_qids = pd.DataFrame(qid_data).sort_values('count', ascending=False).head(top_n)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df_qids['qid'],
        y=df_qids['count'],
        marker=dict(color='#9467bd'),
        hovertemplate='<b>%{x}</b><br>Requests: %{y:,}<extra></extra>'
    ))

    fig.update_layout(
        title=f'Top {top_n} Most Requested Wikidata Items (QIDs)',
        xaxis_title='Wikidata QID',
        yaxis_title='Number of Requests',
        template='plotly_white',
        height=500,
        xaxis={'tickangle': -45}
    )

    return fig


def create_referer_chart(data, top_n=10):
    """Create traffic sources (referer) bar chart"""
    referers = data.get('Referer', {})
    referer_data = [{'referer': ref, 'count': count} for ref, count in referers.items()]
    df_referers = pd.DataFrame(referer_data).sort_values('count', ascending=False).head(top_n)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df_referers['count'],
        y=df_referers['referer'],
        orientation='h',
        marker=dict(color='#d62728'),
        hovertemplate='<b>%{y}</b><br>Requests: %{x:,}<extra></extra>'
    ))

    fig.update_layout(
        title=f'Top {top_n} Traffic Sources',
        xaxis_title='Number of Requests',
        yaxis_title='Referer / User Agent',
        template='plotly_white',
        height=500,
        yaxis={'categoryorder': 'total ascending'}
    )

    return fig


def create_performance_table(df_requests):
    """Create performance summary table"""
    table = dbc.Table.from_dataframe(
        df_requests[['endpoint', 'count', 'min_time', 'avg_time', 'max_time']].round(3),
        striped=True,
        bordered=True,
        hover=True,
        responsive=True,
        style={'fontSize': '0.9rem'}
    )
    return table


# ============================================================================
# LAYOUT BUILDERS
# ============================================================================

def build_overview_tab(data, kpis, df_monthly, df_requests):
    """Build Overview tab content"""
    return dbc.Tab(label='Overview', tab_id='overview', children=[
        dbc.Container([
            # KPI Cards Row
            dbc.Row([
                dbc.Col(create_kpi_card(kpis['total_requests'], 'Total Requests', 'primary'), md=3),
                dbc.Col(create_kpi_card(kpis['unique_users'], 'Unique Users', 'success'), md=3),
                dbc.Col(create_kpi_card(kpis['countries_count'], 'Countries Served', 'info'), md=3),
                dbc.Col(create_kpi_card(kpis['avg_execution_time'], 'Avg Response Time', 'warning'), md=3),
            ], className='mb-4'),

            # Monthly Trend Chart
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=create_monthly_trend_chart(df_monthly))
                ], md=12)
            ], className='mb-4'),

            # Request Performance and Distribution
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=create_request_performance_chart(df_requests))
                ], md=7),
                dbc.Col([
                    dcc.Graph(figure=create_request_distribution_pie(df_requests))
                ], md=5),
            ], className='mb-4'),
        ], fluid=True)
    ])


def build_geography_tab(df_geo, data):
    """Build Geographic Analysis tab content"""
    return dbc.Tab(label='Geographic Analysis', tab_id='geography', children=[
        dbc.Container([
            # Choropleth Map
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=create_geo_choropleth(df_geo))
                ], md=12)
            ], className='mb-4'),

            # Top Countries and Cities
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=create_top_countries_chart(df_geo, top_n=10))
                ], md=6),
                dbc.Col([
                    dcc.Graph(figure=create_top_cities_chart(data, top_n=10))
                ], md=6),
            ], className='mb-4'),
        ], fluid=True)
    ])


def build_performance_tab(df_requests):
    """Build API Performance tab content"""
    return dbc.Tab(label='API Performance', tab_id='performance', children=[
        dbc.Container([
            # Execution Time Box Plot
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=create_execution_time_boxplot(df_requests))
                ], md=12)
            ], className='mb-4'),

            # Performance Table
            dbc.Row([
                dbc.Col([
                    html.H4('Performance Summary Table', className='mb-3'),
                    create_performance_table(df_requests)
                ], md=12)
            ], className='mb-4'),
        ], fluid=True)
    ])


def build_content_tab(data):
    """Build Content Analysis tab content"""
    return dbc.Tab(label='Content Analysis', tab_id='content', children=[
        dbc.Container([
            # Top QIDs
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=create_top_qids_chart(data, top_n=15))
                ], md=12)
            ], className='mb-4'),

            # Traffic Sources
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=create_referer_chart(data, top_n=10))
                ], md=12)
            ], className='mb-4'),
        ], fluid=True)
    ])


# ============================================================================
# MAIN DASHBOARD
# ============================================================================

def create_dashboard():
    """Initialize Dash app with Bootstrap theme and build layout"""
    # Load and process data
    print("Loading data from info.json...")
    data = load_info_json()

    print("Processing data...")
    df_requests = process_request_metrics(data)
    df_geo = process_geographic_data(data)
    df_monthly = process_monthly_usage(data)
    kpis = calculate_kpi_metrics(data)

    print(f"Data loaded successfully:")
    print(f"  - Total Requests: {kpis['total_requests']:,}")
    print(f"  - Unique Users: {kpis['unique_users']:,}")
    print(f"  - Countries: {kpis['countries_count']}")
    print(f"  - Avg Execution Time: {kpis['avg_execution_time']:.2f}s")

    # Initialize Dash app
    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        title='ProVe Analytics Dashboard'
    )

    # Build layout
    app.layout = dbc.Container([
        # Header
        dbc.Row([
            dbc.Col([
                html.H1('ProVe Analytics Dashboard', className='text-center mb-1'),
                html.P('Service Usage Statistics & Performance Metrics',
                      className='text-center text-muted mb-4')
            ])
        ]),

        # Tabs
        dbc.Row([
            dbc.Col([
                dbc.Tabs(id='tabs', active_tab='overview', children=[
                    build_overview_tab(data, kpis, df_monthly, df_requests),
                    build_geography_tab(df_geo, data),
                    build_performance_tab(df_requests),
                    build_content_tab(data),
                ])
            ])
        ]),

        # Footer
        dbc.Row([
            dbc.Col([
                html.Hr(),
                html.P(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                      className='text-center text-muted small')
            ])
        ], className='mt-4')

    ], fluid=True, className='p-4')

    return app


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == '__main__':
    app = create_dashboard()
    print("\n" + "="*60)
    print("Dashboard is running!")
    print("Open your browser and navigate to: http://127.0.0.1:8050")
    print("="*60 + "\n")
    app.run(debug=True, host='127.0.0.1', port=8050)
