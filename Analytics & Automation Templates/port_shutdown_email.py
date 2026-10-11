"""
Asana Task Report Tool
----------------------
Pulls tasks from any Asana project via the API, applies filtering logic
to identify incomplete and late-completed tasks, and sends a formatted
HTML summary email via Outlook.

Requirements:
    pip install pandas requests pywin32

Setup:
    Copy .env.example to .env, fill in your values, and load them into
    your environment before running. Never commit your .env file.

Environment variables:
    ASANA_TOKEN         : Your Asana Personal Access Token
    ASANA_PROJECT_GID   : GID of the Asana project to query
    REPORT_RECIPIENT    : Primary recipient email address
    REPORT_CC           : CC recipients, comma-separated (optional)
    SENDER_NAME         : Name shown in the email sign-off (optional)
    LOOKBACK_DAYS       : How many days back to check (default: 7)
    PARENT_NAME_MIN_LEN : Minimum parent task name length filter (default: 10)

Attribution:
    Data cleaning, filtering, and transformation logic — written by author.
    API pagination, HTML email construction, Outlook dispatch — AI-assisted.
"""

import os
import sys
import pandas as pd
import requests
import win32com.client
from datetime import datetime, timedelta


# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
ASANA_TOKEN         = os.environ.get("ASANA_TOKEN", "")
PROJECT_GID         = os.environ.get("ASANA_PROJECT_GID", "")
RECIPIENT_EMAIL     = os.environ.get("REPORT_RECIPIENT", "")
CC_EMAIL            = os.environ.get("REPORT_CC", "")
SENDER_NAME         = os.environ.get("SENDER_NAME", "The Reporting Tool")
LOOKBACK_DAYS       = int(os.environ.get("LOOKBACK_DAYS", "7"))
PARENT_NAME_MIN_LEN = int(os.environ.get("PARENT_NAME_MIN_LEN", "10"))

# Validate required config before doing any work
_missing = [k for k, v in {
    "ASANA_TOKEN":         ASANA_TOKEN,
    "ASANA_PROJECT_GID":   PROJECT_GID,
    "REPORT_RECIPIENT":    RECIPIENT_EMAIL,
}.items() if not v]

if _missing:
    print(f"[ERROR] Missing required environment variables: {', '.join(_missing)}")
    print("        Set them in your shell or .env file before running.")
    sys.exit(1)


# ──────────────────────────────────────────────────────────────────────────────
# 1. Fetch tasks from Asana          ◀ AI-ASSISTED
#    Handles API pagination automatically, exits cleanly on HTTP errors.
# ──────────────────────────────────────────────────────────────────────────────
def fetch_asana_tasks():
    """Page through the Asana project and return a flat list of task dicts."""
    headers = {"Authorization": f"Bearer {ASANA_TOKEN}"}
    url     = f"https://app.asana.com/api/1.0/projects/{PROJECT_GID}/tasks"
    params  = {
        "opt_fields": "name,due_on,completed,completed_at,created_at,assignee.name,parent.name",
        "limit": 100,
    }

    all_tasks = []
    offset    = None

    while True:
        if offset:
            params["offset"] = offset

        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code != 200:
            print(f"[ERROR] Asana API returned HTTP {response.status_code}")
            print(response.json())
            sys.exit(1)

        data = response.json()
        all_tasks.extend(data["data"])

        next_page = data.get("next_page")
        if next_page:
            offset = next_page.get("offset")
        else:
            break

    print(f"Fetched {len(all_tasks)} tasks from Asana.")
    return all_tasks


# ──────────────────────────────────────────────────────────────────────────────
# 2. Shape raw API response into a DataFrame          ◀ AI-ASSISTED
#    Normalises nested JSON fields (assignee, parent) into flat columns.
# ──────────────────────────────────────────────────────────────────────────────
def tasks_to_dataframe(tasks):
    """Map Asana API fields to a flat DataFrame."""
    rows = []
    for t in tasks:
        assignee = t.get("assignee")
        parent   = t.get("parent")
        rows.append({
            "Name":         t.get("name", ""),
            "Assignee":     assignee.get("name", "") if assignee else "",
            "Created At":   t.get("created_at", ""),
            "Completed At": t.get("completed_at", ""),
            "Due Date":     t.get("due_on", ""),
            "Parent task":  parent.get("name", "") if parent else "",
        })
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────────────────────
# 3. Filter, clean, and transform tasks          ◀ WRITTEN BY Adewole
#
#    Logic:
#      - Drop rows with no assignee or a parent task name below the minimum
#        length threshold (removes section headers / placeholder rows).
#      - Restrict to tasks whose Due Date falls within the lookback window.
#      - Split into two outputs:
#          incomplete_tasks : due in window, not yet completed.
#          overdue_tasks    : due in window, completed after the due date.
#      - Calculate "Days Overdue" for the overdue set.
#      - Format all date columns for clean display.
# ──────────────────────────────────────────────────────────────────────────────
def filter_tasks(df):
    today     = pd.Timestamp.today().normalize()
    yesterday = today - pd.Timedelta(days=1)
    days_ago  = today - pd.Timedelta(days=LOOKBACK_DAYS)

    # Drop rows without an assignee or with a non-descriptive parent task name
    mask_df = df["Assignee"].notnull() & (df["Parent task"].str.len() >= PARENT_NAME_MIN_LEN)
    df = df[mask_df].copy()

    # Parse all date columns in one pass
    for col in ["Completed At", "Created At", "Due Date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    cols = ["Name", "Assignee", "Created At", "Completed At", "Due Date", "Parent task"]
    incomplete_tasks = df[cols].sort_values("Assignee").copy()
    overdue_tasks    = df[cols].sort_values("Assignee").copy()

    # Restrict both sets to the lookback window
    overdue_tasks    = overdue_tasks[overdue_tasks["Due Date"].between(days_ago, yesterday)]
    incomplete_tasks = incomplete_tasks[incomplete_tasks["Due Date"].between(days_ago, yesterday)]

    # Incomplete: no completion timestamp recorded
    incomplete_tasks = incomplete_tasks[incomplete_tasks["Completed At"].isnull()]

    # Overdue: has a completion timestamp AND it falls after the due date
    mask_overdue = (
        overdue_tasks["Completed At"].notnull()
        & (overdue_tasks["Completed At"] > overdue_tasks["Due Date"])
    )
    overdue_tasks = overdue_tasks[mask_overdue]

    # Trim to the columns needed for each output
    incomplete_tasks = incomplete_tasks[["Name", "Assignee", "Due Date", "Parent task"]].copy()
    overdue_tasks    = overdue_tasks[["Name", "Assignee", "Completed At", "Due Date", "Parent task"]].copy()

    # Format dates for display
    incomplete_tasks["Due Date"]  = incomplete_tasks["Due Date"].dt.date
    overdue_tasks["Completed At"] = overdue_tasks["Completed At"].dt.date
    overdue_tasks["Due Date"]     = overdue_tasks["Due Date"].dt.date

    # Calculate days overdue as a clean string (e.g. "3 days")
    overdue_tasks["Days Overdue"] = (
        overdue_tasks["Completed At"].apply(pd.Timestamp)
        - overdue_tasks["Due Date"].apply(pd.Timestamp)
    )
    overdue_tasks["Days Overdue"] = (
        overdue_tasks["Days Overdue"].astype(str).str.split(",").str[0].str.strip()
    )

    # Reset to 1-based index for readable output
    for tdf in [incomplete_tasks, overdue_tasks]:
        tdf.reset_index(drop=True, inplace=True)
        tdf.index = tdf.index + 1

    return incomplete_tasks, overdue_tasks


# ──────────────────────────────────────────────────────────────────────────────
# 4. Build HTML email body          ◀ AI-ASSISTED
#    Generates a two-section HTML table layout — one for incomplete tasks,
#    one for tasks completed after their due date.
# ──────────────────────────────────────────────────────────────────────────────
def build_html(incomplete_tasks, overdue_tasks, cutoff_date):
    html = f"""
<html>
<body style="font-family: Calibri, Arial, sans-serif; font-size: 11pt;">
<p>Hello,</p>
<p>Please find below the task summary report for the last {LOOKBACK_DAYS} days
   (since {cutoff_date.strftime('%d %b %Y')}).</p>

<h3 style="color: #c00000;">&#9888; Incomplete Tasks &mdash; Not Yet Completed ({len(incomplete_tasks)})</h3>
"""

    if len(incomplete_tasks) > 0:
        html += """
<table border="1" cellpadding="6" cellspacing="0"
       style="border-collapse: collapse; font-size: 10pt;">
<tr style="background-color: #c00000; color: white;">
    <th>#</th><th>Task</th><th>Assignee</th><th>Due Date</th><th>Parent Task</th>
</tr>"""
        for idx, row in incomplete_tasks.iterrows():
            html += f"""
<tr>
    <td style="text-align:center;">{idx}</td>
    <td>{row['Name']}</td>
    <td>{row['Assignee']}</td>
    <td>{row['Due Date']}</td>
    <td>{row['Parent task']}</td>
</tr>"""
        html += "</table>"
    else:
        html += "<p style='color: green;'>&#10003; No incomplete tasks &mdash; all caught up!</p>"

    html += f"""
<br>
<h3 style="color: #e67e00;">&#128203; Tasks Completed After Due Date ({len(overdue_tasks)})</h3>
"""

    if len(overdue_tasks) > 0:
        html += """
<table border="1" cellpadding="6" cellspacing="0"
       style="border-collapse: collapse; font-size: 10pt;">
<tr style="background-color: #e67e00; color: white;">
    <th>#</th><th>Task</th><th>Assignee</th><th>Completed At</th>
    <th>Due Date</th><th>Days Overdue</th><th>Parent Task</th>
</tr>"""
        for idx, row in overdue_tasks.iterrows():
            html += f"""
<tr>
    <td style="text-align:center;">{idx}</td>
    <td>{row['Name']}</td>
    <td>{row['Assignee']}</td>
    <td>{row['Completed At']}</td>
    <td>{row['Due Date']}</td>
    <td style="text-align:center; font-weight:bold; color:#e67e00;">{row['Days Overdue']}</td>
    <td>{row['Parent task']}</td>
</tr>"""
        html += "</table>"
    else:
        html += "<p style='color: green;'>&#10003; No late completions in this period.</p>"

    html += f"""
<br>
<p>Kind regards,<br>{SENDER_NAME}</p>
</body>
</html>
"""
    return html


# ──────────────────────────────────────────────────────────────────────────────
# 5. Send email via Outlook          ◀ AI-ASSISTED
#    Dispatches the HTML body through a locally running Outlook instance.
# ──────────────────────────────────────────────────────────────────────────────
def send_email(html_body):
    today_str = datetime.now().strftime("%d %b %Y")
    outlook   = win32com.client.Dispatch("Outlook.Application")
    email     = outlook.CreateItem(0)

    email.To       = RECIPIENT_EMAIL
    email.CC       = CC_EMAIL
    email.Subject  = f"Task Summary Report — {today_str}"
    email.HTMLBody = html_body

    email.Send()
    print(f"Email sent to {RECIPIENT_EMAIL}.")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
if __name__ == "__main__":
    # 1. Pull tasks from Asana
    raw_tasks = fetch_asana_tasks()

    # 2. Normalise into a DataFrame
    df = tasks_to_dataframe(raw_tasks)
    print(f"DataFrame: {len(df)} rows | columns: {list(df.columns)}")

    # 3. Filter and transform (author's logic)
    incomplete_tasks, overdue_tasks = filter_tasks(df)
    print(f"Incomplete tasks : {len(incomplete_tasks)}")
    print(f"Overdue tasks    : {len(overdue_tasks)}")

    # 4. Save to Excel
    # NOTE (GDPR): Output files contain personal data.
    # Store securely and delete per your organisation's retention policy.
    incomplete_tasks.to_excel("incomplete_tasks.xlsx")
    overdue_tasks.to_excel("overdue_tasks.xlsx")
    print("Saved incomplete_tasks.xlsx and overdue_tasks.xlsx")

    # 5. Build and send the email report
    cutoff    = datetime.now().date() - timedelta(days=LOOKBACK_DAYS)
    html_body = build_html(incomplete_tasks, overdue_tasks, cutoff)
    send_email(html_body)
