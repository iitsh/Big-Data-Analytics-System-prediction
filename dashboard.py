#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tableau de Bord Premium de Prédiction du Diabète"""

import streamlit as st
import time
import json
import os
import logging
import threading
import pandas as pd
import numpy as np
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from kafka import KafkaConsumer
from datetime import datetime, timedelta
import warnings

# Supprimer les avertissements
warnings.filterwarnings('ignore')

# Configuration de la page
st.set_page_config(
    page_title="Tableau de Bord de Prédiction du Diabète",
    page_icon="🩸",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Configurer la journalisation
logging.basicConfig(level=logging.ERROR)

# Configuration Kafka
KAFKA_BROKER = "hadoop-master:9092"
KAFKA_TOPIC = "diabetes-data"
KAFKA_GROUP = "streamlit-dashboard-v1"

# Configuration PostgreSQL
PG_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'diabetes',
    'table': 'predictions',
    'user': 'your_user',
    'password': 'your_password'
}

# Fichier de compteur pour les statistiques des messages
COUNTER_FILE = "/tmp/kafka_counter.json"

# Réinitialiser le fichier compteur au démarrage
with open(COUNTER_FILE, 'w') as f:
    json.dump({"total": 0, "per_min": 0, "timestamp": time.time()}, f)

# Générer un ID de session pour suivre uniquement les nouvelles données
if 'session_start_time' not in st.session_state:
    st.session_state.session_start_time = datetime.now()
    st.session_state.record_count = 0
    st.session_state.prev_record_count = 0
    st.session_state.dashboard_start_time = datetime.now()
    # Ajouter cette ligne pour suivre le nombre réel d'enregistrements
    st.session_state.target_record_count = 0

def kafka_counter_thread():
    try:
        with open(COUNTER_FILE, 'r') as f:
            counter = json.load(f)

        total = counter.get("total", 0)
        per_min = 0
        minute_start = time.time()

        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='latest',
            group_id=KAFKA_GROUP,
            consumer_timeout_ms=5000,
            request_timeout_ms=30000,
            session_timeout_ms=10000
        )

        while True:
            message_batch = consumer.poll(timeout_ms=5000)

            message_count = 0
            for tp, messages in message_batch.items():
                message_count += len(messages)

            if message_count > 0:
                total += message_count
                per_min += message_count

                now = time.time()
                if now - minute_start >= 60:
                    per_min = message_count
                    minute_start = now

                with open(COUNTER_FILE, 'w') as f:
                    json.dump({"total": total, "per_min": per_min, "timestamp": now}, f)

            time.sleep(0.1)

    except Exception:
        pass

def start_counter_thread():
    if not any(t.name == "kafka_counter" for t in threading.enumerate()):
        thread = threading.Thread(
            target=kafka_counter_thread,
            daemon=True,
            name="kafka_counter"
        )
        thread.start()

def get_prediction_data():
    try:
        # Récupérer uniquement les données de la session courante
        session_start = st.session_state.session_start_time.strftime('%Y-%m-%d %H:%M:%S')

        conn = psycopg2.connect(
            host=PG_CONFIG['host'],
            port=PG_CONFIG['port'],
            database=PG_CONFIG['database'],
            user=PG_CONFIG['user'],
            password=PG_CONFIG['password'],
            connect_timeout=3
        )

        # Récupérer uniquement les prédictions créées après le démarrage du tableau de bord
        query = f"""
        SELECT gender, age, hypertension, heart_disease, smoking_history,
               bmi, "HbA1c_level", blood_glucose_level,
               actual_diabetes, predicted_diabetes, processing_time
        FROM {PG_CONFIG['table']}
        WHERE processing_time > '{session_start}'::timestamp
        ORDER BY processing_time ASC
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        if len(df) == 0:
            return pd.DataFrame(), "empty", 0

        df['prediction_match'] = df['actual_diabetes'] == df['predicted_diabetes']
        df['processing_time'] = pd.to_datetime(df['processing_time'])

        # Utiliser directement le nombre réel d'enregistrements
        current_count = len(df)
        st.session_state.prev_record_count = st.session_state.record_count
        st.session_state.record_count = current_count

        return df, "postgresql", current_count

    except Exception as e:
        return pd.DataFrame(), "no_data", 0

def calculate_risk_level(row):
    """Calculer le niveau de risque de diabète basé sur plusieurs facteurs"""
    risk_score = 0

    # Facteur de risque IMC
    if row['bmi'] >= 30:
        risk_score += 3
    elif row['bmi'] >= 25:
        risk_score += 2
    elif row['bmi'] >= 23:
        risk_score += 1

    # Facteur de risque glucose sanguin
    if row['blood_glucose_level'] >= 180:
        risk_score += 4
    elif row['blood_glucose_level'] >= 140:
        risk_score += 3
    elif row['blood_glucose_level'] >= 100:
        risk_score += 1

    # Facteur de risque HbA1c
    if row['HbA1c_level'] >= 6.5:
        risk_score += 4
    elif row['HbA1c_level'] >= 5.7:
        risk_score += 2

    # Facteur de risque âge
    if row['age'] >= 65:
        risk_score += 2
    elif row['age'] >= 45:
        risk_score += 1

    # Facteurs de risque additionnels
    if row['hypertension'] == 1:
        risk_score += 2
    if row['heart_disease'] == 1:
        risk_score += 2
    if row['smoking_history'] in ['current', 'former']:
        risk_score += 1

    # Déterminer le niveau de risque
    if risk_score >= 10:
        return "Élevé", risk_score
    elif risk_score >= 6:
        return "Modéré", risk_score
    elif risk_score >= 3:
        return "Faible", risk_score
    else:
        return "Très Faible", risk_score

def calculate_model_metrics(df):
    """Calculer les métriques de performance du modèle"""
    if len(df) == 0:
        return 0, 0, 0, 0, 0

    # Valeurs de la matrice de confusion
    true_positive = ((df['predicted_diabetes'] == 1) & (df['actual_diabetes'] == 1)).sum()
    true_negative = ((df['predicted_diabetes'] == 0) & (df['actual_diabetes'] == 0)).sum()
    false_positive = ((df['predicted_diabetes'] == 1) & (df['actual_diabetes'] == 0)).sum()
    false_negative = ((df['predicted_diabetes'] == 0) & (df['actual_diabetes'] == 1)).sum()

    # Métriques
    accuracy = (true_positive + true_negative) / len(df) * 100

    precision = true_positive / (true_positive + false_positive) if (true_positive + false_positive) > 0 else 0
    precision *= 100

    recall = true_positive / (true_positive + false_negative) if (true_positive + false_negative) > 0 else 0
    recall *= 100

    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    f1 = f1 / 100  # Ajuster à partir du pourcentage

    specificity = true_negative / (true_negative + false_positive) if (true_negative + false_positive) > 0 else 0
    specificity *= 100

    return accuracy, precision, recall, f1, specificity

def main():
    # CSS du tableau de bord premium - éviter les unités px, utiliser rem à la place
    st.markdown("""
    <style>
    /* Base theme */
    .stApp {
        background-color: #111824; /* Légèrement plus clair que l'original */
        color: #FFFFFF; /* Texte plus lumineux */
        font-family: 'Roboto', 'Segoe UI', sans-serif;
    }

    /* Loading animation and glowing effect */
    @keyframes pulse {
        0% {transform: scale(1); opacity: 1;}
        50% {transform: scale(1.05); opacity: 1;}
        100% {transform: scale(1); opacity: 1;}
    }
    .pulse {
        animation: pulse 2s infinite ease-in-out;
    }

    @keyframes glow {
        0% {box-shadow: 0 0 0.3rem rgba(187, 134, 252, 0.3);}
        50% {box-shadow: 0 0 1rem rgba(187, 134, 252, 0.5);}
        100% {box-shadow: 0 0 0.3rem rgba(187, 134, 252, 0.3);}
    }
    .glow {
        animation: glow 3s infinite ease-in-out;
    }

    @keyframes fadeInUp {
        from {opacity: 0; transform: translateY(1rem);}
        to {opacity: 1; transform: translateY(0);}
    }
    .animate-in {
        animation: fadeInUp 0.5s ease-out forwards;
    }

    /* Header styling */
    .premium-header {
        display: flex;
        align-items: center;
        margin-bottom: 1.5rem;
        animation: fadeInUp 0.8s ease-out;
    }
    .header-icon {
        font-size: 2.2rem;
        margin-right: 1rem;
        background: linear-gradient(135deg, #9370DB, #6A35E1);
        width: 3.5rem;
        height: 3.5rem;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        box-shadow: 0 0.3rem 0.8rem rgba(106, 53, 225, 0.3);
    }
    .header-title {
        font-size: 2rem;
        font-weight: 700;
        background: linear-gradient(90deg, #BB86FC, #8559DA);
        -webkit-background-clip: text;
        background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }
    .header-subtitle {
        color: #A4B0C5;
        font-size: 1rem;
        font-weight: 400;
        margin-top: 0.3rem;
    }

    /* Premium metric cards */
    .metric-card {
        background: linear-gradient(135deg, #1E2535, #182030);
        border-radius: 0.8rem;
        padding: 1.2rem;
        margin-bottom: 1rem;
        position: relative;
        overflow: hidden;
        box-shadow: 0 0.3rem 0.8rem rgba(0, 0, 0, 0.2);
        transition: all 0.3s ease;
        border-left: 0.25rem solid #BB86FC;
    }
    .metric-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: linear-gradient(135deg, rgba(187, 134, 252, 0.05), transparent);
        z-index: 1;
        opacity: 0.7; /* Réduire l'opacité de la superposition */
    }
    .metric-card:hover {
        transform: translateY(-0.3rem);
        box-shadow: 0 0.5rem 1.2rem rgba(0, 0, 0, 0.3);
    }
    .metric-label {
        position: relative;
        display: flex;
        align-items: center;
        font-size: 0.9rem;
        color: #A4B0C5;
        margin-bottom: 0.7rem;
        font-weight: 500;
        z-index: 2;
    }
    .metric-icon {
        font-size: 1.1rem;
        margin-right: 0.5rem;
        color: #BB86FC;
    }
    .metric-value {
        position: relative;
        font-size: 2.4rem;
        font-weight: 700;
        color: #BB86FC;
        margin-bottom: 0.3rem;
        z-index: 2;
    }
    .metric-caption {
        position: relative;
        font-size: 0.85rem;
        color: #8893A8;
        z-index: 2;
    }
    .metric-change {
        position: absolute;
        top: 1.2rem;
        right: 1.2rem;
        font-size: 0.85rem;
        font-weight: 500;
        z-index: 2;
    }
    .metric-positive {
        color: #4CAF50;
    }
    .metric-negative {
        color: #F44336;
    }
    .metric-neutral {
        color: #8893A8;
    }

    /* Risk cards */
    .risk-card {
        background: linear-gradient(135deg, #1E2535, #182030);
        border-radius: 0.8rem;
        padding: 1rem;
        margin-bottom: 0.8rem;
        display: flex;
        align-items: center;
        position: relative;
        overflow: hidden;
        box-shadow: 0 0.3rem 0.8rem rgba(0, 0, 0, 0.2);
        transition: all 0.3s ease;
    }
    .risk-card:hover {
        transform: translateX(0.3rem);
        box-shadow: 0 0.3rem 1rem rgba(0, 0, 0, 0.3);
    }
    .risk-high {
        border-left: 0.25rem solid #FF5252;
    }
    .risk-high::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: linear-gradient(135deg, rgba(255, 82, 82, 0.1), transparent);
        z-index: 1;
        opacity: 0.7; /* Réduire l'opacité de superposition */
    }
    .risk-moderate {
        border-left: 0.25rem solid #FFB74D;
    }
    .risk-moderate::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: linear-gradient(135deg, rgba(255, 183, 77, 0.1), transparent);
        z-index: 1;
        opacity: 0.7; /* Réduire l'opacité de superposition */
    }
    .risk-low {
        border-left: 0.25rem solid #64B5F6;
    }
    .risk-low::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: linear-gradient(135deg, rgba(100, 181, 246, 0.1), transparent);
        z-index: 1;
        opacity: 0.7; /* Réduire l'opacité de superposition */
    }
    .risk-very-low {
        border-left: 0.25rem solid #81C784;
    }
    .risk-very-low::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: linear-gradient(135deg, rgba(129, 199, 132, 0.1), transparent);
        z-index: 1;
        opacity: 0.7; /* Réduire l'opacité de superposition */
    }
    .risk-icon {
        font-size: 1.5rem;
        margin-right: 1rem;
        position: relative;
        z-index: 2;
    }
    .risk-content {
        position: relative;
        z-index: 2;
    }
    .risk-title {
        font-weight: 600;
        margin-bottom: 0.2rem;
        color: #E4E8F0;
    }
    .risk-description {
        font-size: 0.8rem;
        color: #A4B0C5;
    }

    /* Kafka counter card */
    .kafka-card {
        background: linear-gradient(135deg, #4A1D96, #7928CA);
        border-radius: 0.8rem;
        padding: 1.5rem;
        color: white;
        margin-bottom: 1rem;
        position: relative;
        overflow: hidden;
        box-shadow: 0 0.3rem 1rem rgba(74, 29, 150, 0.4);
    }
    .kafka-card::before {
        content: '';
        position: absolute;
        top: -50%;
        left: -50%;
        width: 200%;
        height: 200%;
        background: radial-gradient(circle, rgba(255,255,255,0.15) 0%, transparent 60%);
        z-index: 1;
    }
    .kafka-card::after {
        content: '';
        position: absolute;
        top: -10%;
        right: -10%;
        width: 5rem;
        height: 5rem;
        border-radius: 50%;
        background: rgba(255,255,255,0.1);
        z-index: 1;
    }
    .kafka-title {
        position: relative;
        display: flex;
        align-items: center;
        font-size: 1.1rem;
        font-weight: 600;
        opacity: 0.9;
        margin-bottom: 1.2rem;
        z-index: 2;
    }
    .kafka-icon {
        font-size: 1.3rem;
        margin-right: 0.6rem;
    }
    .kafka-value {
        position: relative;
        font-size: 3.2rem;
        font-weight: 700;
        margin-bottom: 0.8rem;
        text-shadow: 0 0.1rem 0.3rem rgba(0,0,0,0.3);
        z-index: 2;
    }
    .kafka-details {
        position: relative;
        display: flex;
        justify-content: space-between;
        font-size: 0.9rem;
        opacity: 0.8;
        z-index: 2;
    }

    /* Alert boxes */
    .alert-box {
        padding: 1rem 1.2rem;
        border-radius: 0.8rem;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
        background: linear-gradient(135deg, #1E2535, #182030);
        box-shadow: 0 0.3rem 0.8rem rgba(0,0,0,0.2);
    }
    .alert-icon {
        font-size: 1.3rem;
        margin-right: 1rem;
    }
    .alert-content {
        flex: 1;
    }
    .alert-title {
        font-weight: 600;
        margin-bottom: 0.3rem;
        color: #E4E8F0;
    }
    .alert-message {
        font-size: 0.9rem;
        color: #A4B0C5;
    }
    .alert-success {
        border-left: 0.25rem solid #4CAF50;
    }
    .alert-success .alert-icon {
        color: #4CAF50;
    }
    .alert-info {
        border-left: 0.25rem solid #2196F3;
    }
    .alert-info .alert-icon {
        color: #2196F3;
    }

    /* Section headers */
    .section-header {
        display: flex;
        align-items: center;
        margin-bottom: 1rem;
        margin-top: 1.5rem;
    }
    .section-icon {
        font-size: 1.3rem;
        margin-right: 0.6rem;
        color: #BB86FC;
    }
    .section-title {
        font-size: 1.3rem;
        font-weight: 600;
        color: #E4E8F0;
    }

    /* Dividers */
    .divider {
        height: 0.15rem;
        background: linear-gradient(90deg, #6A35E1, transparent);
        margin: 0.5rem 0 1.5rem 0;
        border-radius: 0.1rem;
    }

    /* Table styling */
    .stDataFrame div[data-testid="stDataFrameResizable"] {
        max-height: 37.5rem;
        overflow-y: auto;
        background-color: #1E2535;
        border-radius: 0.8rem;
        box-shadow: 0 0.3rem 0.8rem rgba(0,0,0,0.2);
    }

    /* Performance metric boxes */
    .performance-grid {
        display: flex;
        flex-wrap: wrap;
        gap: 1rem;
        margin-bottom: 1.5rem;
    }
    .performance-metric {
        background: linear-gradient(135deg, #1E2535, #182030);
        border-radius: 0.8rem;
        padding: 1rem;
        flex: 1;
        min-width: 8rem;
        text-align: center;
        box-shadow: 0 0.3rem 0.8rem rgba(0,0,0,0.2);
    }
    .performance-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #BB86FC;
        margin-bottom: 0.5rem;
    }
    .performance-label {
        font-size: 0.9rem;
        color: #A4B0C5;
    }

    /* Insight cards */
    .insight-card {
        background: linear-gradient(135deg, #1E2535, #182030);
        border-radius: 0.8rem;
        padding: 1.2rem;
        margin-bottom: 1rem;
        position: relative;
        overflow: hidden;
        box-shadow: 0 0.3rem 0.8rem rgba(0,0,0,0.2);
        border-left: 0.25rem solid #BB86FC;
    }
    .insight-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #E4E8F0;
        margin-bottom: 0.8rem;
        display: flex;
        align-items: center;
    }
    .insight-icon {
        font-size: 1.2rem;
        margin-right: 0.5rem;
        color: #BB86FC;
    }
    .insight-content {
        font-size: 0.95rem;
        color: #A4B0C5;
        line-height: 1.5;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.2rem;
        background-color: #111824;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 0.6rem 1rem;
        color: #A4B0C5;
        background-color: #1E2535;
        border-radius: 0.5rem 0.5rem 0 0;
        font-weight: 500;
    }
    .stTabs [data-baseweb="tab"]:hover {
        background-color: #232A3A;
    }
    .stTabs [aria-selected="true"] {
        color: #E4E8F0 !important;
        background-color: #BB86FC !important;
        font-weight: 600;
    }
    .stTabs [data-baseweb="tab-panel"] {
        background-color: #111824;
    }

    /* Session footer */
    .session-footer {
        background-color: #1E2535;
        border-radius: 0.8rem;
        padding: 0.8rem;
        font-size: 0.8rem;
        color: #A4B0C5;
        text-align: center;
        margin-top: 2rem;
        box-shadow: 0 0.3rem 0.8rem rgba(0,0,0,0.2);
    }

    /* Hide Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .viewerBadge_container__1QSob {display: none;}
    .stDeployButton {display: none;}

    /* Chart container */
    .chart-container {
        background: linear-gradient(135deg, #1E2535, #182030);
        border-radius: 0.8rem;
        padding: 1rem;
        margin-bottom: 1rem;
        box-shadow: 0 0.3rem 0.8rem rgba(0,0,0,0.2);
    }
    </style>
    """, unsafe_allow_html=True)

    # En-tête Premium
    st.markdown("""
    <div class="premium-header">
        <div class="header-icon pulse">🩸</div>
        <div>
            <h1 class="header-title">Tableau de Bord de Prédiction du Diabète</h1>
            <div class="header-subtitle">Surveillance avancée en temps réel des prédictions de diabète utilisant un pipeline de Big Data</div>
        </div>
    </div>
    <div class="divider"></div>
    """, unsafe_allow_html=True)

    # Démarrer le thread d'arrière-plan pour le comptage des messages Kafka
    start_counter_thread()

    # Créer des onglets avec des icônes
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Prédictions", "📈 Visualisation", "⚠️ Analyse des Risques", "🔍 Perspectives"])

    # Lire le compteur de messages
    try:
        with open(COUNTER_FILE, 'r') as f:
            counter = json.load(f)
    except Exception:
        counter = {"total": 0, "per_min": 0, "timestamp": time.time()}

    # Obtenir les données de prédiction de la session courante uniquement
    df, data_source, record_count = get_prediction_data()

    # Calculer les métriques pour tous les onglets
    if not df.empty:
        # Métriques de base
        accuracy, precision, recall, f1, specificity = calculate_model_metrics(df)
        positive_predictions = df['predicted_diabetes'].sum()
        correct_predictions = df['prediction_match'].sum()

        # Métriques de risque
        df['risk_result'] = df.apply(calculate_risk_level, axis=1)
        df['risk_level'] = df['risk_result'].apply(lambda x: x[0])
        df['risk_score'] = df['risk_result'].apply(lambda x: x[1])

        # Métriques basées sur le temps
        df['hour'] = df['processing_time'].dt.hour
    else:
        accuracy = precision = recall = f1 = specificity = 0
        positive_predictions = correct_predictions = 0

    # Métriques de croissance des enregistrements
    record_growth = st.session_state.record_count - st.session_state.prev_record_count
    record_growth_pct = (record_growth / st.session_state.prev_record_count * 100) if st.session_state.prev_record_count > 0 else 0

    # ----- ONGLET 1: APERÇU DES PRÉDICTIONS -----
    with tab1:
        # Statut de connexion
        if data_source == "postgresql":
            st.markdown("""
            <div class="alert-box alert-success">
                <div class="alert-icon">✅</div>
                <div class="alert-content">
                    <div class="alert-title">Connecté à PostgreSQL</div>
                    <div class="alert-message">Réception réussie des données de prédiction en temps réel</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="alert-box alert-info">
                <div class="alert-icon">ℹ️</div>
                <div class="alert-content">
                    <div class="alert-title">Connecté à PostgreSQL</div>
                    <div class="alert-message">En attente de nouvelles prédictions...</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        # Métriques clés dans la première ligne
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            # Indicateur de croissance
            growth_color = "metric-positive" if record_growth > 0 else "metric-neutral"
            growth_icon = "↑" if record_growth > 0 else "―"

            st.markdown(f"""
            <div class='metric-card glow'>
                <div class='metric-change {growth_color}'>{growth_icon} {record_growth_pct:.1f}%</div>
                <div class='metric-label'><span class="metric-icon">📊</span> Enregistrements Traités</div>
                <div class='metric-value'>{record_count}</div>
                <div class='metric-caption'>Nouvelles prédictions de cette session</div>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            st.markdown(f"""
            <div class='metric-card glow'>
                <div class='metric-label'><span class="metric-icon">🎯</span> Précision de Prédiction</div>
                <div class='metric-value'>{accuracy:.1f}%</div>
                <div class='metric-caption'>{correct_predictions} corrects sur {record_count}</div>
            </div>
            """, unsafe_allow_html=True)

        with col3:
            pos_pct = (positive_predictions/record_count*100) if record_count > 0 else 0
            st.markdown(f"""
            <div class='metric-card glow'>
                <div class='metric-label'><span class="metric-icon">⚡</span> Cas Positifs</div>
                <div class='metric-value'>{positive_predictions}</div>
                <div class='metric-caption'>{pos_pct:.1f}% du total des prédictions</div>
            </div>
            """, unsafe_allow_html=True)

        with col4:
            # Temps d'exécution
            uptime = datetime.now() - st.session_state.dashboard_start_time
            hours, remainder = divmod(uptime.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            uptime_str = f"{hours}h {minutes}m {seconds}s"

            st.markdown(f"""
            <div class='metric-card glow'>
                <div class='metric-label'><span class="metric-icon">⏱️</span> Temps de Fonctionnement</div>
                <div class='metric-value'>{uptime_str}</div>
                <div class='metric-caption'>Depuis {st.session_state.dashboard_start_time.strftime('%H:%M:%S')}</div>
            </div>
            """, unsafe_allow_html=True)

        # Ligne des métriques de performance ML
        st.markdown("""
        <div class="section-header">
            <div class="section-icon">📈</div>
            <div class="section-title">Performance du Modèle</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"""
        <div class="performance-grid">
            <div class="performance-metric">
                <div class="performance-value">{accuracy:.1f}%</div>
                <div class="performance-label">Précision</div>
            </div>
            <div class="performance-metric">
                <div class="performance-value">{precision:.1f}%</div>
                <div class="performance-label">Exactitude</div>
            </div>
            <div class="performance-metric">
                <div class="performance-value">{recall:.1f}%</div>
                <div class="performance-label">Rappel</div>
            </div>
            <div class="performance-metric">
                <div class="performance-value">{f1:.2f}</div>
                <div class="performance-label">Score F1</div>
            </div>
            <div class="performance-metric">
                <div class="performance-value">{specificity:.1f}%</div>
                <div class="performance-label">Spécificité</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        col1, col2 = st.columns([3, 1])

        with col1:
            # Section des prédictions récentes
            st.markdown("""
            <div class="section-header">
                <div class="section-icon">📋</div>
                <div class="section-title">Prédictions Récentes</div>
            </div>
            """, unsafe_allow_html=True)

            if not df.empty:
                display_df = df.copy()
                display_df = display_df[[
                    'gender', 'age',
                    'blood_glucose_level', 'HbA1c_level', 'bmi',
                    'actual_diabetes', 'predicted_diabetes', 'prediction_match'
                ]]

                # Formater les données pour l'affichage
                def color_match(val):
                    return 'background-color: rgba(20, 48, 20, 0.7)' if val else 'background-color: rgba(48, 20, 20, 0.7)'

                # Appliquer le formatage et afficher
                styled_df = display_df.style.format({
                    'age': '{:.0f}',
                    'blood_glucose_level': '{:.1f}',
                    'HbA1c_level': '{:.1f}',
                    'bmi': '{:.1f}'
                })
                styled_df = styled_df.map(color_match, subset=['prediction_match'])

                # Afficher le dataframe avec le style
                st.dataframe(styled_df, height=400)
            else:
                st.info("Aucune donnée de prédiction disponible pour l'instant. Démarrez le pipeline producteur et consommateur.")

        with col2:
            # Compteur de messages Kafka avec style premium
            st.markdown(f"""
            <div class='kafka-card'>
                <div class='kafka-title'><span class="kafka-icon">📡</span> Flux de Messages Kafka</div>
                <div class='kafka-value'>{counter['total']}</div>
                <div class='kafka-details'>
                    <div>{counter['per_min']} msgs/min</div>
                    <div>Mis à jour: {datetime.fromtimestamp(counter['timestamp']).strftime('%H:%M:%S')}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            # Carte d'aperçu rapide
            if not df.empty:
                high_risk_count = (df['risk_level'] == 'Élevé').sum()
                avg_risk_score = df['risk_score'].mean()

                st.markdown(f"""
                <div class='insight-card'>
                    <div class='insight-title'><span class="insight-icon">💡</span> Aperçu Rapide</div>
                    <div class='insight-content'>
                        • <strong>{high_risk_count}</strong> patients en catégorie à risque élevé<br>
                        • Score de risque moyen: <strong>{avg_risk_score:.1f}/15</strong><br>
                        • Groupe d'âge le plus courant: <strong>{df['age'].apply(lambda x: f"{(x//10)*10}s").mode()[0]}</strong><br>
                        • Prédiction la plus précise pour les patients avec <strong>IMC {'normal' if df[df['prediction_match']]['bmi'].mean() < 25 else 'élevé'}</strong>
                    </div>
                </div>
                """, unsafe_allow_html=True)

    # ----- ONGLET 2: VISUALISATION DES DONNÉES -----
    with tab2:
        if not df.empty:
            # Ligne des graphiques interactifs
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("""
                <div class="section-header">
                    <div class="section-icon">📊</div>
                    <div class="section-title">Analyse des Facteurs de Risque</div>
                </div>
                """, unsafe_allow_html=True)

                # Graphique à bulles amélioré avec la taille comme score de risque
                color_discrete_map = {
                    "Élevé": "#FF5252",
                    "Modéré": "#FFB74D",
                    "Faible": "#64B5F6",
                    "Très Faible": "#81C784"
                }

                fig = px.scatter(
                    df,
                    x="blood_glucose_level",
                    y="HbA1c_level",
                    size="risk_score",
                    color="risk_level",
                    color_discrete_map=color_discrete_map,
                    hover_data=["age", "bmi", "gender", "risk_score"],
                    opacity=0.8,
                    title="Analyse Interactive des Facteurs de Risque"
                )

                fig.update_layout(
                    template="plotly_dark",
                    plot_bgcolor="rgba(26, 31, 42, 0.8)",
                    paper_bgcolor="rgba(26, 31, 42, 0)",
                    height=450,
                    margin=dict(l=10, r=10, t=50, b=10),
                    font=dict(family="Roboto, sans-serif"),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1,
                        bgcolor="rgba(26, 31, 42, 0.8)",
                        bordercolor="rgba(255, 255, 255, 0.2)",
                        borderwidth=1
                    ),
                    xaxis_title="Niveau de Glucose Sanguin",
                    yaxis_title="Niveau de HbA1c"
                )

                # Ajouter une ligne de référence diagonale pour la zone à haut risque
                fig.add_shape(
                    type="line",
                    x0=100, y0=5.0,
                    x1=200, y1=8.0,
                    line=dict(
                        color="rgba(255, 82, 82, 0.4)",
                        width=2,
                        dash="dash"
                    )
                )

                # Ajouter une annotation pour la zone à haut risque
                fig.add_annotation(
                    x=170, y=7.5,
                    text="Zone à Risque Élevé",
                    showarrow=False,
                    font=dict(color="#FF5252", size=12)
                )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("""
                <div class="section-header">
                    <div class="section-icon">📈</div>
                    <div class="section-title">Distribution de l'IMC par Prédiction de Diabète</div>
                </div>
                """, unsafe_allow_html=True)

                # Histogramme amélioré avec style personnalisé
                fig = px.histogram(
                    df,
                    x="bmi",
                    color="predicted_diabetes",
                    barmode="overlay",
                    nbins=30,
                    opacity=0.7,
                    color_discrete_map={0: "#4C78A8", 1: "#E45756"},
                    marginal="box",
                    title="Distribution de l'IMC avec Boîte à Moustaches"
                )

                fig.update_layout(
                    template="plotly_dark",
                    plot_bgcolor="rgba(26, 31, 42, 0.8)",
                    paper_bgcolor="rgba(26, 31, 42, 0)",
                    height=450,
                    margin=dict(l=10, r=10, t=50, b=10),
                    font=dict(family="Roboto, sans-serif"),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1,
                        bgcolor="rgba(26, 31, 42, 0.8)",
                        bordercolor="rgba(255, 255, 255, 0.2)",
                        borderwidth=1
                    ),
                    xaxis_title="Indice de Masse Corporelle (IMC)",
                    yaxis_title="Nombre"
                )

                # Ajouter des lignes de référence pour les catégories d'IMC
                for bmi_value, label, color in zip(
                    [18.5, 25, 30],
                    ["Insuffisance|Normal", "Normal|Surpoids", "Surpoids|Obésité"],
                    ["#64B5F6", "#FFB74D", "#FF5252"]
                ):
                    fig.add_vline(
                        x=bmi_value,
                        line_dash="dash",
                        line_color=color,
                        annotation_text=label,
                        annotation_position="top"
                    )

                st.plotly_chart(fig, use_container_width=True)

            # Deuxième ligne de visualisations
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("""
                <div class="section-header">
                    <div class="section-icon">📊</div>
                    <div class="section-title">Distribution par Âge selon le Statut Diabétique</div>
                </div>
                """, unsafe_allow_html=True)

                # Distribution par âge avec graphique en violon
                df['age_group'] = pd.cut(
                    df['age'],
                    bins=[0, 30, 40, 50, 60, 70, 100],
                    labels=['<30', '30-40', '40-50', '50-60', '60-70', '70+']
                )

                age_counts = df.groupby(['age_group', 'predicted_diabetes']).size().reset_index(name='count')

                fig = px.bar(
                    age_counts,
                    x="age_group",
                    y="count",
                    color="predicted_diabetes",
                    barmode="group",
                    color_discrete_map={0: "#4C78A8", 1: "#E45756"},
                    title="Distribution par Âge selon la Prédiction de Diabète"
                )

                fig.update_layout(
                    template="plotly_dark",
                    plot_bgcolor="rgba(26, 31, 42, 0.8)",
                    paper_bgcolor="rgba(26, 31, 42, 0)",
                    height=450,
                    margin=dict(l=10, r=10, t=50, b=10),
                    font=dict(family="Roboto, sans-serif"),
                    xaxis_title="Groupe d'Âge",
                    yaxis_title="Nombre",
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1,
                        bgcolor="rgba(26, 31, 42, 0.8)",
                        title="Prédiction de Diabète"
                    )
                )

                # Ajouter des annotations de texte pour le pourcentage
                for i, row in age_counts.iterrows():
                    age_total = age_counts[age_counts['age_group'] == row['age_group']]['count'].sum()
                    percentage = (row['count'] / age_total * 100) if age_total > 0 else 0

                    fig.add_annotation(
                        x=row['age_group'],
                        y=row['count'] + 0.5,
                        text=f"{percentage:.1f}%",
                        showarrow=False,
                        font=dict(
                            color="white",
                            size=10
                        )
                    )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("""
                <div class="section-header">
                    <div class="section-icon">📊</div>
                    <div class="section-title">Exploration 3D des Facteurs de Risque</div>
                </div>
                """, unsafe_allow_html=True)

                # Graphique 3D
                fig = px.scatter_3d(
                    df,
                    x="blood_glucose_level",
                    y="HbA1c_level",
                    z="bmi",
                    color="risk_level",
                    color_discrete_map=color_discrete_map,
                    size="risk_score",
                    opacity=0.8,
                    title="Exploration 3D des Facteurs de Risque"
                )

                fig.update_layout(
                    template="plotly_dark",
                    plot_bgcolor="rgba(26, 31, 42, 0.8)",
                    paper_bgcolor="rgba(26, 31, 42, 0)",
                    height=450,
                    margin=dict(l=10, r=10, t=50, b=10),
                    font=dict(family="Roboto, sans-serif"),
                    scene=dict(
                        xaxis_title="Glucose Sanguin",
                        yaxis_title="HbA1c",
                        zaxis_title="IMC",
                        xaxis=dict(backgroundcolor="rgba(26, 31, 42, 0.8)"),
                        yaxis=dict(backgroundcolor="rgba(26, 31, 42, 0.8)"),
                        zaxis=dict(backgroundcolor="rgba(26, 31, 42, 0.8)")
                    )
                )

                st.plotly_chart(fig, use_container_width=True)

            # Troisième ligne de visualisations - Carte de chaleur de corrélation
            st.markdown("""
            <div class="section-header">
                <div class="section-icon">📊</div>
                <div class="section-title">Analyse de Corrélation des Caractéristiques</div>
            </div>
            """, unsafe_allow_html=True)

            # Préparer les données de corrélation
            corr_data = df[['age', 'bmi', 'blood_glucose_level', 'HbA1c_level', 'risk_score', 'predicted_diabetes']].copy()
            corr_data['predicted_diabetes'] = corr_data['predicted_diabetes'].astype(float)
            corr_matrix = corr_data.corr().round(2)

            # Créer une carte de chaleur de corrélation
            fig = px.imshow(
                corr_matrix,
                text_auto=True,
                color_continuous_scale='RdBu_r',
                zmin=-1, zmax=1,
                aspect="auto",
                title="Carte de Chaleur des Corrélations de Caractéristiques"
            )

            fig.update_layout(
                template="plotly_dark",
                plot_bgcolor="rgba(26, 31, 42, 0.8)",
                paper_bgcolor="rgba(26, 31, 42, 0)",
                height=500,
                margin=dict(l=10, r=10, t=50, b=10),
                font=dict(family="Roboto, sans-serif")
            )

            st.plotly_chart(fig, use_container_width=True)

            # Analyse temporelle des prédictions
            if 'hour' in df.columns:
                st.markdown("""
                <div class="section-header">
                    <div class="section-icon">⏰</div>
                    <div class="section-title">Analyse Temporelle</div>
                </div>
                """, unsafe_allow_html=True)

                # Créer des intervalles horaires
                df['hour_bin'] = df['hour'].apply(lambda x: f"{x:02d}:00")

                # Grouper par heure et compter les prédictions
                hourly_data = df.groupby('hour_bin').agg(
                    total=('predicted_diabetes', 'count'),
                    positive=('predicted_diabetes', 'sum'),
                    correct=('prediction_match', 'sum')
                ).reset_index()

                # Calculer les métriques horaires
                hourly_data['positive_rate'] = (hourly_data['positive'] / hourly_data['total'] * 100).round(1)
                hourly_data['accuracy'] = (hourly_data['correct'] / hourly_data['total'] * 100).round(1)

                # Créer des sous-graphiques pour l'analyse temporelle
                fig = make_subplots(specs=[[{"secondary_y": True}]])

                # Ajouter un diagramme en barres pour le total des prédictions
                fig.add_trace(
                    go.Bar(
                        x=hourly_data['hour_bin'],
                        y=hourly_data['total'],
                        name="Total des Prédictions",
                        marker_color="#4C78A8",
                        opacity=0.7
                    ),
                    secondary_y=False
                )

                # Ajouter un graphique en ligne pour la précision
                fig.add_trace(
                    go.Scatter(
                        x=hourly_data['hour_bin'],
                        y=hourly_data['accuracy'],
                        name="Précision (%)",
                        line=dict(color="#81C784", width=3),
                        mode="lines+markers"
                    ),
                    secondary_y=True
                )

                # Ajouter un graphique en ligne pour le taux positif
                fig.add_trace(
                    go.Scatter(
                        x=hourly_data['hour_bin'],
                        y=hourly_data['positive_rate'],
                        name="Taux Positif (%)",
                        line=dict(color="#E45756", width=3, dash='dash'),
                        mode="lines+markers"
                    ),
                    secondary_y=True
                )

                # Mettre à jour la mise en page
                fig.update_layout(
                    title="Analyse Horaire des Prédictions",
                    template="plotly_dark",
                    plot_bgcolor="rgba(26, 31, 42, 0.8)",
                    paper_bgcolor="rgba(26, 31, 42, 0)",
                    height=500,
                    margin=dict(l=10, r=10, t=50, b=10),
                    font=dict(family="Roboto, sans-serif"),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1,
                        bgcolor="rgba(26, 31, 42, 0.8)",
                        bordercolor="rgba(255, 255, 255, 0.2)",
                        borderwidth=1
                    ),
                    barmode='group'
                )

                # Définir les titres
                fig.update_xaxes(title_text="Heure de la Journée")
                fig.update_yaxes(title_text="Nombre de Prédictions", secondary_y=False)
                fig.update_yaxes(title_text="Pourcentage (%)", secondary_y=True)

                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Aucune donnée disponible pour la visualisation pour l'instant. Démarrez le pipeline producteur et consommateur.")

    # ----- ONGLET 3: ANALYSE DES RISQUES -----
    with tab3:
        st.markdown("""
        <div class="section-header">
            <div class="section-icon">⚠️</div>
            <div class="section-title">Analyse des Risques de Diabète</div>
        </div>
        """, unsafe_allow_html=True)

        if not df.empty:
            # Compter par niveau de risque
            risk_counts = df['risk_level'].value_counts().reset_index()
            risk_counts.columns = ['Risk Level', 'Count']

            # S'assurer que tous les niveaux de risque sont représentés
            all_risks = ['Élevé', 'Modéré', 'Faible', 'Très Faible']
            for risk in all_risks:
                if risk not in risk_counts['Risk Level'].values:
                    risk_counts = pd.concat([risk_counts, pd.DataFrame([{'Risk Level': risk, 'Count': 0}])], ignore_index=True)

            # Trier par gravité
            risk_order = {'Élevé': 0, 'Modéré': 1, 'Faible': 2, 'Très Faible': 3}
            risk_counts['sort_order'] = risk_counts['Risk Level'].map(risk_order)
            risk_counts = risk_counts.sort_values('sort_order').drop('sort_order', axis=1)

            # Calculer les pourcentages
            total = risk_counts['Count'].sum()
            risk_counts['Percentage'] = (risk_counts['Count'] / total * 100).round(1)

            # Afficher la distribution des risques
            col1, col2 = st.columns([3, 2])

            with col1:
                # Graphique en anneau premium pour la distribution des risques
                colors = {'Élevé': '#FF5252', 'Modéré': '#FFB74D', 'Faible': '#64B5F6', 'Très Faible': '#81C784'}

                fig = px.pie(
                    risk_counts,
                    values='Count',
                    names='Risk Level',
                    color='Risk Level',
                    color_discrete_map=colors,
                    hole=0.6,
                    title="Distribution des Niveaux de Risque des Patients"
                )

                # Mettre à jour la mise en page pour un aspect premium
                fig.update_layout(
                    template="plotly_dark",
                    plot_bgcolor="rgba(26, 31, 42, 0.8)",
                    paper_bgcolor="rgba(26, 31, 42, 0)",
                    font=dict(family="Roboto, sans-serif"),
                    height=450,
                    margin=dict(l=10, r=10, t=50, b=10),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.2,
                        xanchor="center",
                        x=0.5
                    )
                )

                # Ajouter du texte au milieu du graphique en anneau
                fig.add_annotation(
                    text=f"{total}<br>Patients",
                    x=0.5, y=0.5,
                    font_size=20,
                    font_family="Roboto, sans-serif",
                    font_color="white",
                    showarrow=False
                )

                # Ajouter un modèle de survol personnalisé
                fig.update_traces(
                    textinfo='percent+label',
                    hovertemplate='<b>%{label}</b><br>Nombre: %{value}<br>Pourcentage: %{percent:.1%}<extra></extra>'
                )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Explications des niveaux de risque avec cartes améliorées
                st.markdown("<h3 style='color: #E4E8F0; font-size: 1.2rem; margin-bottom: 1rem;'>Définitions des Niveaux de Risque</h3>", unsafe_allow_html=True)

                # Risque élevé - maintenant avec nombre et pourcentage
                high_count = risk_counts[risk_counts['Risk Level'] == 'Élevé']['Count'].iloc[0]
                high_pct = risk_counts[risk_counts['Risk Level'] == 'Élevé']['Percentage'].iloc[0]

                st.markdown(f"""
                <div class="risk-card risk-high">
                    <div class="risk-icon">⚠️</div>
                    <div class="risk-content">
                        <div class="risk-title">Risque Élevé <span style="float: right; font-size: 0.9rem;">{high_count} patients ({high_pct}%)</span></div>
                        <div class="risk-description">Plusieurs facteurs de risque présents. Consultation médicale immédiate recommandée. Score de risque ≥ 10 sur 15.</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                # Risque modéré
                mod_count = risk_counts[risk_counts['Risk Level'] == 'Modéré']['Count'].iloc[0]
                mod_pct = risk_counts[risk_counts['Risk Level'] == 'Modéré']['Percentage'].iloc[0]

                st.markdown(f"""
                <div class="risk-card risk-moderate">
                    <div class="risk-icon">⚠️</div>
                    <div class="risk-content">
                        <div class="risk-title">Risque Modéré <span style="float: right; font-size: 0.9rem;">{mod_count} patients ({mod_pct}%)</span></div>
                        <div class="risk-description">Plusieurs indicateurs préoccupants. Changements de mode de vie et surveillance conseillés. Score de risque entre 6-9 sur 15.</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                # Risque faible
                low_count = risk_counts[risk_counts['Risk Level'] == 'Faible']['Count'].iloc[0]
                low_pct = risk_counts[risk_counts['Risk Level'] == 'Faible']['Percentage'].iloc[0]

                st.markdown(f"""
                <div class="risk-card risk-low">
                    <div class="risk-icon">ℹ️</div>
                    <div class="risk-content">
                        <div class="risk-title">Risque Faible <span style="float: right; font-size: 0.9rem;">{low_count} patients ({low_pct}%)</span></div>
                        <div class="risk-description">Quelques facteurs à surveiller. Maintenir des habitudes saines et des contrôles réguliers. Score de risque entre 3-5 sur 15.</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                # Risque très faible
                vlow_count = risk_counts[risk_counts['Risk Level'] == 'Très Faible']['Count'].iloc[0]
                vlow_pct = risk_counts[risk_counts['Risk Level'] == 'Très Faible']['Percentage'].iloc[0]

                st.markdown(f"""
                <div class="risk-card risk-very-low">
                    <div class="risk-icon">✅</div>
                    <div class="risk-content">
                        <div class="risk-title">Risque Très Faible <span style="float: right; font-size: 0.9rem;">{vlow_count} patients ({vlow_pct}%)</span></div>
                        <div class="risk-description">Indicateurs minimaux présents. Continuer un mode de vie sain pour maintenir ce statut. Score de risque entre 0-2 sur 15.</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            # Analyse de contribution des facteurs de risque
            st.markdown("""
            <div class="section-header">
                <div class="section-icon">📊</div>
                <div class="section-title">Analyse de la Contribution des Facteurs de Risque</div>
            </div>
            """, unsafe_allow_html=True)

            # Créer une analyse des facteurs de risque
            col1, col2 = st.columns(2)

            with col1:
                # Graphique radar pour les facteurs de risque
                # Préparer les données pour le graphique radar
                risk_levels = ['Élevé', 'Modéré', 'Faible', 'Très Faible']
                metrics = ['bmi', 'blood_glucose_level', 'HbA1c_level', 'age', 'hypertension', 'heart_disease']
                labels = ['IMC', 'Glucose Sanguin', 'HbA1c', 'Âge', 'Hypertension', 'Maladie Cardiaque']

                # Normaliser les données pour le graphique radar
                radar_data = []

                for risk in risk_levels:
                    risk_df = df[df['risk_level'] == risk]
                    if len(risk_df) > 0:
                        values = []
                        for metric in metrics:
                            if metric in ['hypertension', 'heart_disease']:
                                # Pour les colonnes binaires, prendre le taux de 1s
                                value = risk_df[metric].mean()
                            else:
                                # Pour les variables continues, normaliser à la plage 0-1
                                min_val = df[metric].min()
                                max_val = df[metric].max()
                                mean_val = risk_df[metric].mean()
                                value = (mean_val - min_val) / (max_val - min_val) if (max_val - min_val) > 0 else 0
                            values.append(value)
                        radar_data.append(values)
                    else:
                        radar_data.append([0] * len(metrics))

                # Créer un graphique radar
                fig = go.Figure()

                colors = ['#FF5252', '#FFB74D', '#64B5F6', '#81C784']

                for i, risk in enumerate(risk_levels):
                    # Analyser la couleur hex en composants RGB
                    r = int(colors[i][1:3], 16)
                    g = int(colors[i][3:5], 16)
                    b = int(colors[i][5:7], 16)

                    # Créer une chaîne rgba appropriée
                    fill_color = f'rgba({r}, {g}, {b}, 0.2)'

                    fig.add_trace(go.Scatterpolar(
                        r=radar_data[i] + [radar_data[i][0]],  # Fermer la boucle
                        theta=labels + [labels[0]],  # Fermer la boucle
                        fill='toself',
                        name=risk,
                        line_color=colors[i],
                        fillcolor=fill_color
                    ))

                fig.update_layout(
                    title="Contribution des Facteurs de Risque par Niveau de Risque",
                    template="plotly_dark",
                    plot_bgcolor="rgba(26, 31, 42, 0.8)",
                    paper_bgcolor="rgba(26, 31, 42, 0)",
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            range=[0, 1]
                        )
                    ),
                    font=dict(family="Roboto, sans-serif"),
                    height=450,
                    margin=dict(l=80, r=80, t=50, b=50),
                    showlegend=True,
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.1,
                        xanchor="center",
                        x=0.5
                    )
                )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Distribution des scores de risque
                fig = px.histogram(
                    df,
                    x='risk_score',
                    color='risk_level',
                    color_discrete_map=color_discrete_map,
                    marginal="box",
                    nbins=16,
                    opacity=0.7,
                    title="Distribution des Scores de Risque par Niveau de Risque"
                )

                # Ajouter des lignes verticales pour les seuils de niveau de risque
                fig.add_vline(x=3, line_dash="dash", line_color="#64B5F6", annotation_text="Risque Faible", annotation_position="top")
                fig.add_vline(x=6, line_dash="dash", line_color="#FFB74D", annotation_text="Risque Modéré", annotation_position="top")
                fig.add_vline(x=10, line_dash="dash", line_color="#FF5252", annotation_text="Risque Élevé", annotation_position="top")

                fig.update_layout(
                    template="plotly_dark",
                    plot_bgcolor="rgba(26, 31, 42, 0.8)",
                    paper_bgcolor="rgba(26, 31, 42, 0)",
                    font=dict(family="Roboto, sans-serif"),
                    height=450,
                    margin=dict(l=10, r=10, t=50, b=10),
                    xaxis_title="Score de Risque (0-15)",
                    yaxis_title="Nombre",
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1,
                        bgcolor="rgba(26, 31, 42, 0.8)"
                    )
                )

                st.plotly_chart(fig, use_container_width=True)

            # Distribution des risques par données démographiques
            st.markdown("""
            <div class="section-header">
                <div class="section-icon">👥</div>
                <div class="section-title">Distribution des Risques par Données Démographiques</div>
            </div>
            """, unsafe_allow_html=True)

            col1, col2 = st.columns(2)

            with col1:
                # Risque par genre
                gender_risk = pd.crosstab(df['gender'], df['risk_level'])

                # Calculer les pourcentages
                gender_risk_pct = gender_risk.div(gender_risk.sum(axis=1), axis=0) * 100

                # Créer un diagramme à barres empilées
                fig = px.bar(
                    gender_risk_pct.reset_index().melt(id_vars=['gender'], var_name='risk_level', value_name='percentage'),
                    x="gender",
                    y="percentage",
                    color="risk_level",
                    color_discrete_map=color_discrete_map,
                    title="Distribution des Niveaux de Risque par Genre"
                )

                fig.update_layout(
                    template="plotly_dark",
                    plot_bgcolor="rgba(26, 31, 42, 0.8)",
                    paper_bgcolor="rgba(26, 31, 42, 0)",
                    font=dict(family="Roboto, sans-serif"),
                    height=400,
                    margin=dict(l=10, r=10, t=50, b=10),
                    xaxis_title="Genre",
                    yaxis_title="Pourcentage (%)",
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1,
                        bgcolor="rgba(26, 31, 42, 0.8)"
                    )
                )

                # Ajouter des étiquettes de données
                for i, level in enumerate(gender_risk_pct.columns):
                    for j, gender in enumerate(gender_risk_pct.index):
                        value = gender_risk_pct.loc[gender, level]
                        if value >= 5:  # Afficher uniquement les étiquettes pour les valeurs >= 5%
                            fig.add_annotation(
                                x=gender,
                                y=gender_risk_pct.loc[gender, :].iloc[:i].sum() + value/2,
                                text=f"{value:.1f}%",
                                showarrow=False,
                                font=dict(color="white", size=10)
                            )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Risque par groupe d'âge
                age_risk = pd.crosstab(df['age_group'], df['risk_level'])

                # Calculer les pourcentages
                age_risk_pct = age_risk.div(age_risk.sum(axis=1), axis=0) * 100

                # Créer une carte de chaleur
                fig = px.imshow(
                    age_risk_pct,
                    labels=dict(x="Niveau de Risque", y="Groupe d'Âge", color="Pourcentage (%)"),
                    x=age_risk_pct.columns,
                    y=age_risk_pct.index,
                    color_continuous_scale='Blues',
                    text_auto='.1f',
                    aspect="auto",
                    title="Distribution des Niveaux de Risque par Groupe d'Âge (%)"
                )

                fig.update_layout(
                    template="plotly_dark",
                    plot_bgcolor="rgba(26, 31, 42, 0.8)",
                    paper_bgcolor="rgba(26, 31, 42, 0)",
                    font=dict(family="Roboto, sans-serif"),
                    height=400,
                    margin=dict(l=10, r=10, t=50, b=10)
                )

                fig.update_traces(texttemplate='%{text}%', textfont=dict(color="white"))

                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Aucune donnée disponible pour l'analyse des risques pour l'instant. Démarrez le pipeline producteur et consommateur.")

    # ----- ONGLET 4: PERSPECTIVES -----
    with tab4:
        st.markdown("""
        <div class="section-header">
            <div class="section-icon">📝</div>
            <div class="section-title">Points Clés & Recommandations</div>
        </div>
        """, unsafe_allow_html=True)

        if not df.empty:
            # Créer des perspectives basées sur les données
            col1, col2 = st.columns(2)

            with col1:
                # Perspectives de performance du modèle
                st.markdown("""
                <div class='insight-card'>
                    <div class='insight-title'><span class="insight-icon">📈</span> Analyse de Performance du Modèle</div>
                    <div class='insight-content'>
                        Le modèle actuel de prédiction du diabète démontre une performance solide avec des métriques clés:
                        <ul>
                            <li><strong>Précision:</strong> Le modèle prédit correctement le statut du diabète dans la plupart des cas, avec une marge d'amélioration pour certains groupes démographiques</li>
                            <li><strong>Exactitude vs. Rappel:</strong> Le modèle semble prioriser l'identification des vrais positifs, ce qui est approprié pour les applications de dépistage médical</li>
                            <li><strong>Domaines d'Amélioration:</strong> La performance du modèle pourrait être améliorée pour les patients avec des métriques limites (HbA1c 5.7-6.4)</li>
                        </ul>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                # Perspectives des facteurs de risque
                high_risk_count = (df['risk_level'] == 'Élevé').sum()
                high_risk_pct = (high_risk_count / len(df) * 100).round(1)

                # Calculer les facteurs les plus influents pour le risque élevé
                high_risk_df = df[df['risk_level'] == 'Élevé']
                if len(high_risk_df) > 0:
                    avg_glucose = high_risk_df['blood_glucose_level'].mean().round(1)
                    avg_hba1c = high_risk_df['HbA1c_level'].mean().round(1)
                    avg_bmi = high_risk_df['bmi'].mean().round(1)
                    htn_rate = (high_risk_df['hypertension'].mean() * 100).round(1)
                else:
                    avg_glucose = avg_hba1c = avg_bmi = htn_rate = 0

                st.markdown(f"""
                <div class='insight-card'>
                    <div class='insight-title'><span class="insight-icon">⚠️</span> Analyse des Facteurs de Risque</div>
                    <div class='insight-content'>
                        <p><strong>{high_risk_count}</strong> patients ({high_risk_pct}%) sont actuellement dans la catégorie à risque élevé. Les facteurs de risque clés dans cette population incluent:</p>
                        <ul>
                            <li><strong>Glucose Sanguin:</strong> Moyenne {avg_glucose} mg/dL (plage normale: 70-99 mg/dL)</li>
                            <li><strong>HbA1c:</strong> Moyenne {avg_hba1c}% (plage normale: en dessous de 5.7%)</li>
                            <li><strong>IMC:</strong> Moyenne {avg_bmi} (plage saine: 18.5-24.9)</li>
                            <li><strong>Hypertension:</strong> Présente chez {htn_rate}% des patients à risque élevé</li>
                        </ul>
                        <p>Ces résultats suggèrent que le contrôle de la glycémie et la gestion du poids devraient être les cibles principales d'intervention.</p>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            with col2:
                # Perspectives démographiques
                gender_high_risk = df[df['risk_level'] == 'Élevé']['gender'].value_counts()
                most_at_risk_gender = gender_high_risk.idxmax() if not gender_high_risk.empty else "Inconnu"

                age_high_risk = df[df['risk_level'] == 'Élevé']['age_group'].value_counts()
                most_at_risk_age = age_high_risk.idxmax() if not age_high_risk.empty else "Inconnu"

                st.markdown(f"""
                <div class='insight-card'>
                    <div class='insight-title'><span class="insight-icon">👥</span> Tendances Démographiques</div>
                    <div class='insight-content'>
                        <p>L'analyse révèle d'importantes tendances démographiques dans le risque de diabète:</p>
                        <ul>
                            <li><strong>Distribution par Genre:</strong> Les patients {most_at_risk_gender} montrent une représentation plus élevée dans la catégorie à risque élevé</li>
                            <li><strong>Facteur Âge:</strong> Le groupe d'âge {most_at_risk_age} démontre le profil de risque de diabète le plus élevé</li>
                            <li><strong>Facteurs Composés:</strong> Lorsque l'âge et un IMC élevé sont présents ensemble, les scores de risque augmentent significativement</li>
                        </ul>
                        <p>Ces résultats peuvent aider à cibler les programmes de dépistage et d'intervention vers les démographies à plus haut risque.</p>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                # Recommandations
                st.markdown("""
                <div class='insight-card'>
                    <div class='insight-title'><span class="insight-icon">💡</span> Recommandations</div>
                    <div class='insight-content'>
                        <p>Sur la base de l'analyse de données actuelle, les actions suivantes sont recommandées:</p>
                        <ol>
                            <li><strong>Dépistage Amélioré:</strong> Prioriser le dépistage pour les patients avec plusieurs facteurs de risque, particulièrement ceux avec une glycémie et un IMC élevés</li>
                            <li><strong>Programmes d'Intervention:</strong> Développer des interventions ciblées se concentrant sur la gestion du poids et le contrôle de la glycémie pour les patients dans la catégorie à risque modéré</li>
                            <li><strong>Affinement du Modèle:</strong> Envisager de recalibrer le modèle de prédiction pour améliorer la performance dans les cas limites et les groupes démographiques spécifiques</li>
                            <li><strong>Enrichissement des Données:</strong> Collecter des facteurs de style de vie supplémentaires tels que les habitudes alimentaires et d'exercice pour améliorer la précision de la prédiction des risques</li>
                        </ol>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            # Visualisation de l'arbre de décision
            st.markdown("""
            <div class="section-header">
                <div class="section-icon">🌳</div>
                <div class="section-title">Chemin de Décision pour la Classification des Risques</div>
            </div>
            """, unsafe_allow_html=True)

            # Créer une représentation visuelle de la logique de calcul des risques
            fig = go.Figure()

            # Ajouter des nœuds
            nodes = [
                {"id": 0, "label": "Patient", "level": 0, "x": 0, "y": 0},
                # Niveau 1 - IMC
                {"id": 1, "label": "IMC ≥ 30", "level": 1, "x": -3, "y": -1, "score": "+3", "color": "#FF5252"},
                {"id": 2, "label": "IMC 25-29.9", "level": 1, "x": -1, "y": -1, "score": "+2", "color": "#FFB74D"},
                {"id": 3, "label": "IMC 23-24.9", "level": 1, "x": 1, "y": -1, "score": "+1", "color": "#64B5F6"},
                {"id": 4, "label": "IMC < 23", "level": 1, "x": 3, "y": -1, "score": "+0", "color": "#81C784"},
                # Niveau 2 - Glucose sanguin
                {"id": 5, "label": "Glucose ≥ 180", "level": 2, "x": -3.5, "y": -2, "score": "+4", "color": "#FF5252"},
                {"id": 6, "label": "Glucose 140-179", "level": 2, "x": -2, "y": -2, "score": "+3", "color": "#FFB74D"},
                {"id": 7, "label": "Glucose 100-139", "level": 2, "x": 0, "y": -2, "score": "+1", "color": "#64B5F6"},
                {"id": 8, "label": "Glucose < 100", "level": 2, "x": 2, "y": -2, "score": "+0", "color": "#81C784"},
                # Niveau 3 - HbA1c
                {"id": 9, "label": "HbA1c ≥ 6.5", "level": 3, "x": -3, "y": -3, "score": "+4", "color": "#FF5252"},
                {"id": 10, "label": "HbA1c 5.7-6.4", "level": 3, "x": -1, "y": -3, "score": "+2", "color": "#FFB74D"},
                {"id": 11, "label": "HbA1c < 5.7", "level": 3, "x": 1, "y": -3, "score": "+0", "color": "#81C784"},
                # Niveau 4 - Âge
                {"id": 12, "label": "Âge ≥ 65", "level": 4, "x": -2, "y": -4, "score": "+2", "color": "#FFB74D"},
                {"id": 13, "label": "Âge 45-64", "level": 4, "x": 0, "y": -4, "score": "+1", "color": "#64B5F6"},
                {"id": 14, "label": "Âge < 45", "level": 4, "x": 2, "y": -4, "score": "+0", "color": "#81C784"},
                # Niveau 5 - Autres facteurs de risque
                {"id": 15, "label": "Hypertension", "level": 5, "x": -2, "y": -5, "score": "+2", "color": "#FFB74D"},
                {"id": 16, "label": "Maladie Cardiaque", "level": 5, "x": 0, "y": -5, "score": "+2", "color": "#FFB74D"},
                {"id": 17, "label": "Tabagisme", "level": 5, "x": 2, "y": -5, "score": "+1", "color": "#64B5F6"},
                # Niveau 6 - Catégories de risque final
                {"id": 18, "label": "RISQUE ÉLEVÉ\nScore ≥ 10", "level": 6, "x": -3, "y": -6.5, "color": "#FF5252", "size": 1.5},
                {"id": 19, "label": "RISQUE MODÉRÉ\nScore 6-9", "level": 6, "x": -1, "y": -6.5, "color": "#FFB74D", "size": 1.5},
                {"id": 20, "label": "RISQUE FAIBLE\nScore 3-5", "level": 6, "x": 1, "y": -6.5, "color": "#64B5F6", "size": 1.5},
                {"id": 21, "label": "RISQUE TRÈS FAIBLE\nScore 0-2", "level": 6, "x": 3, "y": -6.5, "color": "#81C784", "size": 1.5}
            ]

            # Ajouter un titre
            fig.update_layout(
                title="Arbre de Décision pour le Calcul du Risque de Diabète",
                titlefont=dict(size=20),
                title_x=0.5
            )

            # Ajouter des arêtes sous forme de lignes
            edges = [
                # De la racine au niveau 1
                (0, 1), (0, 2), (0, 3), (0, 4),
                # Connecter tous aux niveaux de score
                (1, 18), (1, 19), (1, 20), (1, 21),
                (2, 18), (2, 19), (2, 20), (2, 21),
                (3, 19), (3, 20), (3, 21),
                (4, 19), (4, 20), (4, 21),
                # Connecter les niveaux de glucose aux scores
                (5, 18), (5, 19),
                (6, 18), (6, 19),
                (7, 19), (7, 20),
                (8, 20), (8, 21),
                # Connecter les niveaux de HbA1c aux scores
                (9, 18), (9, 19),
                (10, 19), (10, 20),
                (11, 20), (11, 21),
                # Connecter les groupes d'âge aux scores
                (12, 19), (12, 20),
                (13, 20), (13, 21),
                (14, 20), (14, 21),
                # Connecter les autres facteurs de risque aux scores
                (15, 19), (15, 20),
                (16, 19), (16, 20),
                (17, 20), (17, 21)
            ]

            # Ajouter des nœuds à la figure
            for node in nodes:
                node_size = node.get('size', 1) * 20

                fig.add_trace(go.Scatter(
                    x=[node['x']],
                    y=[node['y']],
                    mode='markers+text',
                    marker=dict(
                        symbol='circle',
                        size=node_size,
                        color=node.get('color', 'rgba(71, 58, 131, 0.8)'),
                        line=dict(color='rgba(255, 255, 255, 0.5)', width=1)
                    ),
                    text=node['label'],
                    textposition='middle center',
                    hoverinfo='text',
                    hovertext=f"{node['label']}{' (Score: ' + node['score'] + ')' if 'score' in node else ''}",
                    name=str(node['id'])
                ))

            # Ajouter des lignes pour les arêtes
            for edge in edges:
                source, target = edge
                source_node = next(node for node in nodes if node['id'] == source)
                target_node = next(node for node in nodes if node['id'] == target)

                fig.add_trace(go.Scatter(
                    x=[source_node['x'], target_node['x']],
                    y=[source_node['y'], target_node['y']],
                    mode='lines',
                    line=dict(color='rgba(180, 180, 180, 0.4)', width=1),
                    hoverinfo='none',
                    showlegend=False
                ))

            # Mettre à jour la mise en page
            fig.update_layout(
                template="plotly_dark",
                plot_bgcolor="rgba(26, 31, 42, 0.8)",
                paper_bgcolor="rgba(26, 31, 42, 0)",
                font=dict(family="Roboto, sans-serif"),
                height=600,
                margin=dict(l=20, r=20, t=60, b=20),
                showlegend=False,
                xaxis=dict(
                    showgrid=False,
                    zeroline=False,
                    showticklabels=False,
                    range=[-4, 4]
                ),
                yaxis=dict(
                    showgrid=False,
                    zeroline=False,
                    showticklabels=False,
                    range=[-7, 0.5],
                    scaleanchor="x",
                    scaleratio=1
                )
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Aucune donnée disponible pour les perspectives pour l'instant. Démarrez le pipeline producteur et consommateur.")

    # Informations de session dans le pied de page
    st.markdown(f"""
    <div class='session-footer'>
        <span style="opacity: 0.8;">Démarrage du tableau de bord:</span> {st.session_state.session_start_time.strftime('%Y-%m-%d %H:%M:%S')} |
        <span style="opacity: 0.8;">Enregistrements traités:</span> {record_count} |
        <span style="opacity: 0.8;">Temps d'exécution:</span> {str(datetime.now() - st.session_state.dashboard_start_time).split('.')[0]}
    </div>
    """, unsafe_allow_html=True)

    # Rafraîchissement automatique avec délai de 3 secondes
    time.sleep(3)
    st.rerun()

if __name__ == "__main__":
    main()
