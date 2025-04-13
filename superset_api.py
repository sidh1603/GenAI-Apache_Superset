import os
import json
import requests
from urllib.parse import urljoin
from dotenv import load_dotenv
import time
import uuid

load_dotenv(dotenv_path=".env", override=True)


class SupersetAPI:
    def __init__(self):
        self.base_url = os.getenv("SUPERSET_URL", "http://localhost:8088").rstrip("/")
        self.username = os.getenv("SUPERSET_USERNAME", "admin")
        self.password = os.getenv("SUPERSET_PASSWORD", "admin")
        self.database = os.getenv("SUPERSET_DATABASE", "mydata")
        self.table = os.getenv("SUPERSET_TABLE", "mock_data1")
        self.session = requests.Session()
        self.access_token = None
        self.csrf_token = None
        self.dataset_id = None
        self.datetime_column = None
        self.numeric_columns = []
        self.all_columns = []
        self.login()

    def login(self):
        url = urljoin(self.base_url, "/api/v1/security/login")
        payload = {
            "username": self.username,
            "password": self.password,
            "provider": "db",
            "refresh": True
        }
        r = self.session.post(url, json=payload)
        r.raise_for_status()
        self.access_token = r.json()["access_token"]
        self.session.headers.update({
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        self.csrf_token = self.get_csrf_token()
        self.session.headers.update({
            "X-CSRFToken": self.csrf_token,
            "Referer": self.base_url
        })

    def get_csrf_token(self):
        url = urljoin(self.base_url, "/api/v1/security/csrf_token/")
        r = self.session.get(url)
        r.raise_for_status()
        return r.json()["result"]

    def get_dataset_id(self):
        page = 0
        while True:
            url = f"{self.base_url}/api/v1/dataset/?q=(page:{page},page_size:100)"
            r = self.session.get(url)
            r.raise_for_status()
            for dataset in r.json()["result"]:
                if dataset["table_name"] == self.table and dataset["database"]["database_name"] == self.database:
                    self.dataset_id = dataset["id"]
                    self._extract_columns()
                    print(f"[DEBUG] Found dataset ID: {self.dataset_id}")
                    return self.dataset_id
            if not r.json()["result"]:
                break
            page += 1
        raise ValueError(f"Dataset '{self.table}' in database '{self.database}' not found.")

    def _extract_columns(self):
        url = urljoin(self.base_url, f"/api/v1/dataset/{self.dataset_id}")
        r = self.session.get(url)
        r.raise_for_status()
        columns = r.json()["result"]["columns"]
        self.all_columns = [col["column_name"] for col in columns]
        for col in columns:
            if col.get("is_dttm"):
                self.datetime_column = col["column_name"]
            if col["type_generic"] in [0, 1, 2]:
                self.numeric_columns.append(col["column_name"])

    def create_chart(self, name, chart_type, groupby=[], metrics=[], query=None):
        chart_type_map = {
            "bar": "bar",
            "line": "line",
            "area": "area",
            "pie": "pie"
        }
        viz_type = chart_type_map.get(chart_type, chart_type)
        if not self.dataset_id:
            self.get_dataset_id()

        default_metric_col = self.numeric_columns[0] if self.numeric_columns else groupby[0] if groupby else "id"
        formatted_metrics = []

        for m in metrics:
            if isinstance(m, str):
                formatted_metrics.append({
                    "expressionType": "SIMPLE",
                    "column": {"column_name": default_metric_col},
                    "aggregate": m.upper(),
                    "label": m
                })
            elif isinstance(m, dict):
                formatted_metrics.append({
                    "expressionType": "SIMPLE",
                    "column": m.get("column", {"column_name": default_metric_col}),
                    "aggregate": m.get("aggregate", "COUNT").upper(),
                    "label": m.get("label", "count")
                })

        if not formatted_metrics:
            formatted_metrics = [{
                "expressionType": "SIMPLE",
                "column": {"column_name": default_metric_col},
                "aggregate": "COUNT",
                "label": "count"
            }]

        params = {
            "viz_type": viz_type,
            "datasource": f"{self.dataset_id}__table",
            "metrics": formatted_metrics,
            "groupby": groupby,
            "row_limit": 1000,
            "adhoc_filters": [],
            "order_desc": True,
            "contribution": False,
            "show_legend": True,
            "query_mode": "aggregate",
            "force": True,
            "sort_by_metric": True
        }

        if viz_type in ["line", "area"] and self.datetime_column:
            params.update({
                "granularity_sqla": self.datetime_column,
                "time_column": self.datetime_column,
                "time_grain_sqla": "P1D",
                "time_range": "No filter",
                "time_range_endpoints": ["inclusive", "exclusive"]
            })

        if viz_type == "pie":
            params.update({
                "metric": formatted_metrics[0],
                "number_format": ".0f",
                "show_labels": True,
                "donut": False
            })
        elif viz_type == "bar":
            params.update({
                "x_axis": groupby[0] if groupby else None,
                "x_axis_sort_asc": True,
                "show_bar_value": True,
                "y_axis_format": ".0f"
            })
        elif viz_type == "line":
            params.update({
                "x_axis": groupby[0] if groupby else None,
                "show_markers": True,
                "y_axis_format": ".0f"
            })
        elif viz_type == "area":
            params.update({
                "x_axis": groupby[0] if groupby else None,
                "y_axis_format": ".0f",
                "contribution": False,
                "show_controls": True
            })

        if query:
            params["sql"] = query
            params["query_mode"] = "raw"

        payload = {
            "slice_name": name,
            "viz_type": viz_type,
            "datasource_id": self.dataset_id,
            "datasource_type": "table",
            "params": json.dumps(params),
            "dashboards": []
        }

        print("[DEBUG] Chart Payload:", json.dumps(params, indent=2))
        r = self.session.post(urljoin(self.base_url, "/api/v1/chart/"), json=payload)
        r.raise_for_status()
        return r.json()["id"]

    def create_dashboard(self, title, chart_ids):
        self.csrf_token = self.get_csrf_token()
        self.session.headers.update({"X-CSRFToken": self.csrf_token})
        url = urljoin(self.base_url, "/api/v1/dashboard/")

        position_json = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
            "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]}
        }

        for i, chart_id in enumerate(chart_ids):
            chart_uuid = str(uuid.uuid4())
            chart_key = f"CHART-{chart_id}"
            wrapper_key = f"GRID_ELEMENT-{chart_id}"
            position_json[chart_key] = {
                "type": "CHART",
                "id": chart_key,
                "meta": {"uuid": chart_uuid, "chartId": chart_id, "width": 4, "height": 50},
                "children": [],
                "parents": [wrapper_key]
            }
            position_json[wrapper_key] = {
                "type": "GRID_ELEMENT",
                "id": wrapper_key,
                "children": [chart_key],
                "parents": ["ROOT_ID", "GRID_ID"],
                "meta": {"width": 4, "height": 50},
                "position": {
                    "col": (i % 3) * 4,
                    "row": (i // 3) * 50,
                    "width": 4,
                    "height": 50
                }
            }
            position_json["GRID_ID"]["children"].append(wrapper_key)

        payload = {
            "dashboard_title": title,
            "slug": f"{title.lower().replace(' ', '-')}-{int(time.time())}",
            "position_json": json.dumps(position_json),
            "css": "",
            "json_metadata": json.dumps({
                "timed_refresh_immune_slices": [],
                "expanded_slices": {},
                "filter_scopes": {},
                "default_filters": "{}",
                "color_scheme": "supersetColors",
                "label_colors": {},
                "chart_configuration": {str(cid): {} for cid in chart_ids}
            }),
            "published": True,
            "owners": [1]
        }

        r = self.session.post(url, json=payload)
        r.raise_for_status()
        return r.json()["id"]

    def get_dashboard_url(self, dashboard_id):
        return f"{self.base_url}/superset/dashboard/{dashboard_id}/"
