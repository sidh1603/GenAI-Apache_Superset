import streamlit as st
import pandas as pd
import os, glob, ast
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from superset_api import SupersetAPI
import re
import time
import base64

load_dotenv()
st.set_page_config(page_title="Verizon AI Dashboard Generator", layout="wide")

# Custom background with dark verizon theme
def set_background(image_file):
    with open(image_file, "rb") as image:
        encoded = base64.b64encode(image.read()).decode()
    bg_css = f"""
    <style>
        .stApp {{
            background-image: url("data:image/jpg;base64,{encoded}");
            background-size: cover;
            background-position: center;
            background-attachment: fixed;
            color: white;
        }}
        .verizon-logo {{
            display: block;
            left: 15px;
            
            top: -20px;
            margin-bottom: -10px;
            width: 80px;
        }}
        .verizon-title {{
            text-align: center;
            color: #ff0000;
            font-size: 38px;
            font-weight: bold;
            margin-top: 10px;
            margin-bottom: 30px;
        }}
        .stTextArea > label, .stFileUploader > label {{
            color: #ff4d4d;
            font-weight: bold;
        }}
    </style>
    """
    st.markdown(bg_css, unsafe_allow_html=True)

# Set background and logo
set_background("images/bg.jpg")
st.markdown(f'<img src="data:image/png;base64,{base64.b64encode(open("images/logo.png", "rb").read()).decode()}" class="verizon-logo">', unsafe_allow_html=True)
st.markdown('<div class="verizon-title">Verizon AI Dashboard Generator</div>', unsafe_allow_html=True)

# Clean chart folder
chart_dir = os.path.join(os.getcwd(), "charts")
os.makedirs(chart_dir, exist_ok=True)
for file in glob.glob(f"{chart_dir}/*.png"):
    os.remove(file)

# Model
llm = ChatOpenAI(model="gpt-4", temperature=0)

uploaded_file = st.file_uploader("Upload mock_data CSV", type=["csv"])
if uploaded_file:
    df = pd.read_csv(uploaded_file)
    st.subheader("📄 Sample of Uploaded Data")
    st.dataframe(df.head())

    query = st.text_area("💬 Describe the dashboard or chart to generate")

    if st.button("Save to Superset") and query:
        st.info("🤖 Talking to the AI to generate chart definitions...")

        prompt = PromptTemplate.from_template("""
        You are an AI assistant helping create dashboards in Apache Superset.
        The user asked: {query}
        Dataset columns: {columns}
        Respond with a Python list of dictionaries like:
        [
          {{
            "chart_type": "bar", 
            "groupby": "state", 
            "metric": {{"label": "count", "aggregate": "COUNT"}}, 
            "title": "Device Count by State"
          }}
        ]
        Do not explain anything. Return ONLY the Python list.
        """)
        
        try:
            instruction_query = prompt.format(query=query, columns=list(df.columns))
            response = llm.invoke(instruction_query)
            chart_instructions = ast.literal_eval(response.content)

            if not isinstance(chart_instructions, list):
                raise ValueError("Response is not a valid chart list.")

            st.success("✅ Chart definitions generated. Sending to Superset...")
            superset = SupersetAPI()
            chart_ids = []

            for i, chart in enumerate(chart_instructions):
                try:
                    chart_type = chart.get("chart_type", "bar")
                    groupby = [chart["groupby"]] if isinstance(chart.get("groupby"), str) else chart.get("groupby", [])
                    metric = chart.get("metric", {"label": "count", "aggregate": "COUNT"})
                    title = chart.get("title", f"Chart {i+1}")

                    for col in groupby:
                        if col not in df.columns:
                            raise ValueError(f"Column '{col}' not found in dataset")

                    chart_id = superset.create_chart(title, chart_type, groupby, [metric])
                    chart_ids.append(chart_id)
                    st.success(f"✅ Created chart: {title} (ID: {chart_id})")

                except Exception as e:
                    st.error(f"❌ Failed to create chart {i+1}: {str(e)}")
                    continue

            if not chart_ids:
                raise ValueError("No charts were created")

            dashboard_title = f"Dashboard: {re.sub(r'[^a-zA-Z0-9_ ]', '', query)[:40]}"
            dashboard_id = superset.create_dashboard(dashboard_title, chart_ids)
            st.success("🎉 Dashboard created successfully!")
            st.markdown(f"[🔗 View Dashboard]({superset.get_dashboard_url(dashboard_id)})", unsafe_allow_html=True)
            st.balloons()

        except Exception as e:
            st.error("❌ Failed to generate dashboard.")
            st.exception(e)
