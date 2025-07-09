# Some tools for Daily review
# Version 2 - 2025-Apr-06

# use shift + alt + A to comment out a block of code



 #        Example directory for script run:                   C:\Users\RobAdair\Documents\python\base_daily_view

# renamed to rdaq_prod_review.py prior to checking into git.  (totally_basic_ver2.py is the original name)

import pandas as pd
import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import streamlit as st
from dotenv import load_dotenv
import plotly.express as px
import plotly.graph_objs as go
#from root_metadata import carrier_color_dict
from plotly.subplots import make_subplots
import time
import plotly.io as pio
import heavy_lifts   #offshore bulky queries
pio.templates.default = 'plotly'

load_dotenv()

# credentials and string for db connection
def get_rsr_conn():
	host_str = os.getenv("RSR_CONN")
	engine = create_engine(host_str)
	return engine

st.set_page_config(page_title='Market Review Tool',
   page_icon='🧷',
   layout='wide')


st.title("Production Market Daily Review 🛸")

#Pull initial CSID into the webapp and all to select a new CSID
with st.form("my_form"):
    csid = st.number_input("Enter CSID [All Tabs]:", min_value=1, max_value=10000000, value=12610)
    submitted = st.form_submit_button("Submit")
        
if submitted:
    df_eom = heavy_lifts.get_eom(csid)
    st.write("Flagged Market level comparisons [Market checkpoint 1a]")
    st.write(df_eom)

if submitted:
    df_eom_full = heavy_lifts.get_eom_full(csid)
    st.write("Full Market level comparisons")
    st.write(df_eom_full)

    
# Turn off call net comparison for now...
#if submitted:
#    df_callnet = heavy_lifts.get_callnet(csid)
#    st.write("Call Net - NOT filtered")
#    st.write(df_callnet)

if submitted:
    df_market_net = heavy_lifts.get_marketnet(csid)
    st.write("Market-level network comparisons - NOT filtered (source auto-schema)")
    st.write(df_market_net)

if submitted:
    df_datadiff = heavy_lifts.get_datadiff(csid)
    st.write("Filtered daily differences in data/call tests [Market checkpoint 1b]")
    st.write(df_datadiff) 
 
if submitted:
    get_madish = heavy_lifts.get_MADish(csid)
    st.write("MAD-type tables plots [Market checkpoint 1c]")
    #st.write(get_madish)               #commented out for now

# Begin code for plotting MAD-type data
    if submitted and get_madish is not None:
        # Create new column for collection set grouping
        get_madish['period'] = get_madish['collection_set'].apply(lambda x: '2024-2H' if x.endswith('2024-2H') else '2025-1H' if x.endswith('2025-1H') else 'Other')
        
        # Create subplots using fixed 'acc' metric
        fig = px.scatter(get_madish, 
                        x='carrier', 
                        y='acc',
                        color='period',
                        facet_col='test_type_id',
                        symbol='period',
                        title='Access Values by Test Type and Carrier',
                        labels={'acc': 'access Value', 'carrier': 'Carrier'},
                        height=600,
                        hover_data=['loc_day', 'n_grp'])
                # Update layout for better readability
        fig.update_layout(showlegend=True)
        fig.update_xaxes(tickangle=45)
        
        # Display the plot
        st.plotly_chart(fig, use_container_width=True)
        
        fig = px.scatter(get_madish, 
                        x='carrier', 
                        y='task',
                        color='period',
                        facet_col='test_type_id',
                        symbol='period',
                        title='Task Values by Test Type and Carrier',
                        labels={'task': 'Task Value', 'carrier': 'Carrier'},
                        height=600,
                        hover_data=['loc_day', 'n_grp'])        
        
        # Update layout for better readability
        fig.update_layout(showlegend=True)
        fig.update_xaxes(tickangle=45)
        
        # Display the plot
        st.plotly_chart(fig, use_container_width=True)

# Create filtered dataframes for each test type
        df_19 = get_madish[get_madish['test_type_id'] == 19]
        df_20 = get_madish[get_madish['test_type_id'] == 20]
        df_26 = get_madish[get_madish['test_type_id'] == 26]

        # Create a figure with 4 subplots
        fig = make_subplots(rows=2, cols=2, 
               subplot_titles=('Upload Speed (Test 19)', 
                 'Download Speed (Test 20)',
                 'LDRs Access Speed 95p (Test 26)',
                 'LDRs Task Speed 95p (Test 26)'))

        # Define marker properties for each period
        marker_props = {
            '2025-1H': dict(symbol='circle', size=12),
            '2024-2H': dict(symbol='triangle-up', size=8)
        }

        # Add traces for each subplot
        for period in df_19['period'].unique():
            if period in marker_props:  # Only plot 2024-2H and 2025-1H
                df_temp = df_19[df_19['period'] == period]
                fig.add_trace(
                    go.Scatter(x=df_temp['carrier'], y=df_temp['ul_speed_50p'],
                          mode='markers', name=f'Period {period} - Test 19',
                          marker=marker_props[period],
                          showlegend=True,hovertext=df_temp['loc_day']),
                    row=1, col=1
                )

        for period in df_20['period'].unique():
            if period in marker_props:
                df_temp = df_20[df_20['period'] == period]
                fig.add_trace(
                    go.Scatter(x=df_temp['carrier'], y=df_temp['dl_speed_50p'],
                          mode='markers', name=f'Period {period} - Test 20',
                          marker=marker_props[period],
                          showlegend=True,hovertext=df_temp['loc_day']),
                    row=1, col=2
                )

        for period in df_26['period'].unique():
            if period in marker_props:
                df_temp = df_26[df_26['period'] == period]
                fig.add_trace(
                    go.Scatter(x=df_temp['carrier'], y=df_temp['ldrs_access_sp_95p'],
                          mode='markers', name=f'Period {period} - Test 26 Access',
                          marker=marker_props[period],
                          showlegend=True,hovertext=df_temp['loc_day']),
                    row=2, col=1
                )
                fig.add_trace(
                    go.Scatter(x=df_temp['carrier'], y=df_temp['ldrs_task_sp_95p'],
                          mode='markers', name=f'Period {period} - Test 26 Task',
                          marker=marker_props[period],
                          showlegend=True,hovertext=df_temp['loc_day']),
                    row=2, col=2
                )

        # Update layout
        fig.update_layout(height=800, width=1000, title_text="Speed Metrics by Test Type")
        fig.update_xaxes(tickangle=45)

        # Display the plot
        st.plotly_chart(fig, use_container_width=True)

if submitted:
    df_dev_algo = heavy_lifts.get_algo(csid)
    st.write("Device algorithm: 4+ consecutive failures [Market checkpoint 2]")
    st.write(df_dev_algo)

if submitted:
    df_layer3_reveiw = heavy_lifts.get_layer3_m2m(csid)
    st.write("M2M call failures flagged for layer 3 review [Market checkpoint 3]")
    st.write(df_layer3_reveiw)

if submitted:
    df_dqcheck = heavy_lifts.get_dqcheck(csid)
    st.write("DQ check items [Market checkpoint 4]")
    st.write(df_dqcheck)

if submitted:
    df_bl_by_test = heavy_lifts.get_bl_test(csid)
    st.write("Review the rate of blocklisting by test type")
    st.write(df_bl_by_test)

if submitted:
    df_bl = heavy_lifts.get_excluded(csid)
    st.write("Data Exclusion Review")
    st.write(df_bl)



