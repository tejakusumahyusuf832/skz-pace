<div align="center">
  
  # 🚀 SKZ PACE: Predictive Analytics & Content Ecosystem
  
  [![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://www.python.org/)
  [![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-336791.svg)](https://www.postgresql.org/)
  [![Airflow](https://img.shields.io/badge/Orchestration-Apache_Airflow-017CEE.svg)](https://airflow.apache.org/)
  [![Transformers](https://img.shields.io/badge/NLP-HuggingFace-FFD21E.svg)](https://huggingface.co/)
  [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)



  *Bridging the gap between raw YouTube metadata and actionable digital strategy for Stray Kids.*

</div>

---

## 📌 Executive Summary
⚠️⚠️⚠️ *Note: This portfolio project is a personal initiative and is **not affiliated with JYP Entertainment or Stray Kids**. All data used is publicly available on YouTube.*

**The Problem:** Despite boasting a digital footprint of nearly 10 billion views, Stray Kids' YouTube content strategy relied on superficial native analytics. JYP Entertainment lacked granular, semantic insights into how non-music content (vlogs, reality shows, teasers) influenced fandom retention, sentiment, and long-term channel momentum, leading to production inefficiencies.  

**The Solution:** SKZ PACE is a phased, full-stack data science initiative. It features a decoupled ELT pipeline orchestrating daily metadata ingestion, alongside a fine-tuned Hugging Face NLP model (XLM-RoBERTa) to extract multilingual sentiment from over 200,000 top comments. *Note: The project is currently in Phase 2, with predictive machine learning modules actively in development.*

**The Impact (So Far):** * Engineered a sentiment analysis pipeline with an **87.76% accuracy rate**.
* Identified "SKZ CODE" as the loyalty engine with a **18-day half-life**.
* Quantified the "Viral Dilution Effect," proving mathematically that algorithmic viral reach inversely correlates with core fandom engagement rates.

---

## 📊 Visual Walkthrough

<!-- <details>
<summary><b> 📸 Click to view Project Visuals </b></summary>
<br> -->

| 📈 Interactive Power BI Dashboard | ⚙️ Predictive Scenario Simulator (WIP) |
| :---: | :---: |
| <img src="reports/dashboards/dashboard-demo.gif" alt="Power BI Dashboard Animation" width="400"/> | [Coming Up Soon] <img src="https://via.placeholder.com/400x250.png?text=Streamlit+App+(Coming+Soon)" alt="Streamlit API Placeholder" />|
| *Visualizing content pillar engagement and lifetime velocity.* | *(Coming in Phase 4) Simulating A/B production scenarios.* |

<!-- </details> -->

---

## 🛠️ Architecture & Tech Stack

This project utilizes a completely free, open-source tech stack while maintaining enterprise-level orchestration.

**1. Data Engineering & ELT (Phase 1 - Complete)**
* **Orchestration:** GitHub Actions (Cron), Dockerized Apache Airflow
* **Databases:** Neon Serverless (Cloud Raw), PostgreSQL (Local Transformed)
* **Processing:** Polars, NumPy

**2. Data Analysis & NLP (Phase 2 - Complete)**
* **Language & Embeddings:** Python, Hugging Face Transformers (`twitter-xlm-roberta-base-sentiment`)
* **Ground-Truth Labeling:** Gemini Pro Extended API
* **Visualization:** Power BI, Plotly, Seaborn

**3. Machine Learning & Deployment (Phase 3 & 4 - In Development)**
* **Modeling:** XGBoost, Scikit-learn
* **Tracking & Compute:** MLflow (via DagsHub), Google Colab GPUs
* **Backend & UI:** FastAPI, Streamlit, Docker, Hugging Face Spaces

---

## 🔬 Key Strategic Insights (EDA)

Through rigorous exploratory data analysis, several actionable business intelligence insights were extracted:

1. **The Loyalty Engines (Variety Shows vs. Passion Projects):** Exponential decay modeling ($V(t) = V_0 \cdot e^{-\lambda t} + c$) reveals that "SKZ CODE" videos demonstrate an extraordinary half-life of 18 days. "SKZ RECORD/PLAYER" and "Making Film" videos create a continuous, positive-sentiment algorithmic loop alongside official music videos.
2. **The Format Advantage:** YouTube Shorts drive a significantly higher baseline engagement rate due to lower friction UI and algorithmically induced high-dopamine scrolling.
3. **The Viral Dilution Effect:** There is a moderate negative correlation between `daily_view_velocity` and `lifetime_engagement_rate`. As videos break out of the core fandom ("STAY") and reach the general public, views surge but interaction ratios drop mathematically.
4. **The Legacy Resurgence:** 25 legacy videos (2+ years old) are currently generating more marginal engagement than releases from the last 30 days, heavily indicating external catalysts (like TikTok audio trends) driving new fans to the back-catalog.

---

## 📚 Deep Dive Documentation

Explore these detailed technical documentations below for an in-depth look at how this system was built:

* 🗺️ **[Project Management Brief & Roadmap](references/1.0-yt-project-management.md):** The original business case, problem diagnosis, and phase-by-phase execution plan.
* 🗂️ **[Repository Organization](references/2.0-yt-project-organization.md):** A detailed map of the monolithic repository structure.
* 📖 **[Data Dictionary](references/skz_data_dictionary.html):** Schema definitions, metric calculations, and feature engineering logic.
* 💻 **[Local Deployment Guide](references/[INSERT_DEPLOYMENT_GUIDE_FILENAME.md]):** Step-by-step instructions for developers to spin up the Airflow containers and database environments locally.

---

## 🔮 Future Work (Phases 3 - 5)

The foundation is set. The next evolution of SKZ PACE involves moving from *descriptive* analytics to *predictive* modeling:

* **Predictive A/B Simulator:** Training a Time-Decay Weighted regression model to forecast the engagement rate of proposed video concepts based on category, tags, and publish time.
* **Semantic Embeddings:** Generating text embeddings for video metadata using open-source models (`EmbeddingGemma` or `Qwen3-Embedding`).
* **Interactive Web App:** Wrapping the ML model in a FastAPI endpoint and serving it to stakeholders via a containerized Streamlit UI deployed on Hugging Face Spaces.

---

## 🤝 Let's Connect!

If you are interested in digital strategy, machine learning engineering, or data science ecosystems, feel free to reach out:

* **LinkedIn:** [Yusuf Tejakusumah](https://www.linkedin.com/in/tejakusumahyusuf832/)
* **Email:** [tejakusumahyusuf832@gmail.com](mailto:yousiftejakusoumah@gmail.com)
* **Role:** Full-Stack Data Scientist / Project Manager

*(If you found this architecture or analysis insightful, please consider leaving a ⭐ on the repository!)*