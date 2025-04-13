import streamlit as st
import pandas as pd
import os, glob, ast
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from superset_api import SupersetAPI
import re
import time

load_dotenv()

st.set_page_config(page_title="AI Dashboard Generator", layout="wide")
st.title("\U0001F4CA AI Chart & Dashboard Creator")

llm = ChatOpenAI(model="gpt-4", temperature=0)
chart_dir = os.path.join(os.getcwd(), "charts")
os.makedirs(chart_dir, exist_ok=True)
for file in glob.glob(f"{chart_dir}/*.png"):
    os.remove(file)

uploaded_file = st.file_uploader("Upload mock_data CSV", type=["csv"])
if uploaded_file:
    df = pd.read_csv(uploaded_file)
    st.subheader("\U0001F4C4 Sample of Data")
    st.dataframe(df.head())

    query = st.text_area("Describe the dashboard or ask for specific charts (e.g. 'Show device count by make as pie chart')")

    if st.button("Save to Superset") and query:
        st.info("Talking to the AI to generate chart definitions...")

        chart_instruction_prompt = PromptTemplate.from_template("""
        You are an AI assistant helping create dashboards in Apache Superset.
        The user asked: {query}
        Dataset columns: {columns}
        Respond with a Python list of dictionaries with chart definitions:
        [
          {{
            "chart_type": "bar", 
            "groupby": "state", 
            "metric": {{"label": "count", "aggregate": "COUNT"}}, 
            "title": "Device Count by State"
          }},
          {{
            "chart_type": "pie", 
            "groupby": "make", 
            "metric": {{"label": "count", "aggregate": "COUNT"}}, 
            "title": "Distribution by Make"
          }}
        ]
        IMPORTANT: Only use columns that exist in the dataset.
        The metric should always be a dictionary with 'label' and 'aggregate' keys.
        Supported chart types: bar, pie, line, histogram.
        Do not explain anything. Return ONLY the Python list.
        """)

        instruction_query = chart_instruction_prompt.format(
            query=query,
            columns=list(df.columns)
        )

        try:
            response = llm.invoke(instruction_query)
            chart_instructions = ast.literal_eval(response.content)

            if not isinstance(chart_instructions, list) or not all(isinstance(c, dict) for c in chart_instructions):
                raise ValueError("AI response is not a valid list of chart dictionaries.")

            st.success("✅ Chart definitions generated. Sending to Superset...")

            superset = SupersetAPI()
            chart_ids = []
            
            for i, chart in enumerate(chart_instructions):
                try:
                    chart_type = chart.get("chart_type", "bar")
                    groupby = chart.get("groupby", [])
                    if isinstance(groupby, str):
                        groupby = [groupby]
                    
                    metric = chart.get("metric", {"label": "count", "aggregate": "COUNT"})
                    title = chart.get("title", f"Chart {i+1}")
                    
                    # Verify columns exist in dataframe
                    for col in groupby:
                        if col not in df.columns:
                            raise ValueError(f"Column '{col}' not found in dataset")
                    
                    chart_id = superset.create_chart(
                        name=title,
                        chart_type=chart_type,
                        groupby=groupby,
                        metrics=[metric]
                    )
                    chart_ids.append(chart_id)
                    st.write(f"Created chart: {title} (ID: {chart_id})")
                except Exception as e:
                    st.error(f"Failed to create chart {i+1}: {str(e)}")
                    continue

            if not chart_ids:
                raise ValueError("No charts were created successfully")

            
            time.sleep(2)
            # Create a clean dashboard title
            clean_query = re.sub(r"[^a-zA-Z0-9_ ]", "", query)
            dashboard_title = f"Dashboard: {clean_query[:40]}".strip()
            
            dashboard_id = superset.create_dashboard(dashboard_title, chart_ids)
            dash_url = superset.get_dashboard_url(dashboard_id)

            st.success("\U0001F389 Dashboard created successfully!")
            st.markdown(f"[\U0001F517 View on Superset]({dash_url})", unsafe_allow_html=True)

        except Exception as e:
            st.error(f"❌ Failed to generate dashboard: {str(e)}")
            st.exception(e)