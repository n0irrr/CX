import os
import time

import boto3
import pandas as pd
import streamlit as st

DATABASE = os.environ.get("ATHENA_DATABASE", "cx_issues")
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "cx-pipeline")

athena = boto3.client("athena")


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    qid = athena.start_query_execution(
        QueryString=sql, QueryExecutionContext={"Database": DATABASE}, WorkGroup=WORKGROUP
    )["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if status["State"] not in ("QUEUED", "RUNNING"):
            break
        time.sleep(0.5)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(status.get("StateChangeReason", status["State"]))

    pages = athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid)
    rows = [[c.get("VarCharValue") for c in r["Data"]] for p in pages for r in p["ResultSet"]["Rows"]]
    df = pd.DataFrame(rows[1:], columns=rows[0])
    for col in df.columns:
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.notna().sum() == df[col].notna().sum():
            df[col] = numeric
    return df


st.set_page_config(page_title="Commissioning Issues", layout="wide")
st.title("Commissioning issue tracker")

days = st.sidebar.selectbox("Ingested in the last", [7, 30, 90, 365], index=2, format_func=lambda d: f"{d} days")
window = f"ingest_date >= date_format(current_date - interval '{int(days)}' day, '%Y-%m-%d')"

kpi = query(
    f"""
    SELECT count(*) AS total,
           count_if(status = 'open') AS open_issues,
           count_if(status = 'open' AND severity = 'critical') AS critical_open
    FROM issues WHERE {window}
    """
).iloc[0]
quarantined = query(f"SELECT count(*) AS n FROM quarantine WHERE {window}").iloc[0]["n"]
received = kpi["total"] + quarantined

c1, c2, c3, c4 = st.columns(4)
c1.metric("Issues", int(kpi["total"]))
c2.metric("Open", int(kpi["open_issues"]))
c3.metric("Critical open", int(kpi["critical_open"]))
c4.metric("Quarantine rate", f"{quarantined / received:.1%}" if received else "n/a")

left, right = st.columns(2)
with left:
    st.subheader("Issues by root cause")
    st.bar_chart(
        query(f"SELECT category, count(*) AS issues FROM issues WHERE {window} GROUP BY 1 ORDER BY 2 DESC"),
        x="category",
        y="issues",
        horizontal=True,
    )
with right:
    st.subheader("Open issues by equipment and severity")
    sev = query(
        f"""
        SELECT equipment_type, severity, count(*) AS issues
        FROM issues WHERE {window} AND status = 'open' GROUP BY 1, 2
        """
    )
    if not sev.empty:
        st.bar_chart(sev, x="equipment_type", y="issues", color="severity", stack=True)

left, right = st.columns(2)
with left:
    st.subheader("Mean days to close, by contractor")
    st.dataframe(
        query(
            f"""
            SELECT contractor,
                   round(avg(date_diff('day', CAST(opened_at AS date), CAST(closed_at AS date))), 1)
                       AS mean_days_to_close,
                   count(*) AS closed_issues
            FROM issues WHERE {window} AND status = 'closed'
            GROUP BY 1 ORDER BY 2 DESC
            """
        ),
        hide_index=True,
    )
with right:
    st.subheader("Top quarantine reasons")
    st.dataframe(
        query(
            f"""
            SELECT reason, count(*) AS records
            FROM quarantine CROSS JOIN UNNEST(errors) AS t(reason)
            WHERE {window} GROUP BY 1 ORDER BY 2 DESC LIMIT 10
            """
        ),
        hide_index=True,
    )

st.subheader("Classification method")
st.dataframe(
    query(f"SELECT classified_by, count(*) AS issues FROM issues WHERE {window} GROUP BY 1 ORDER BY 2 DESC"),
    hide_index=True,
)
