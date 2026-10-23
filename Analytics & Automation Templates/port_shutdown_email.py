import pandas as pd 
import numpy as np 
import datetime as dt
import requests
import win32com.client
from datetime import date, timedelta, datetime
import json
import requests

# ──────────────────────────────────────────────────────────────────────────────
# 1-3. Configuration, Paginated API Fetch & DataFrame Assembly  ◀ WRITTEN BY Adewole
#
# Logic:
# - Three hard-coded constants (ASANA_TOKEN, GROUP_ID, REPORT_RECIPIENT) sit
#   at the top to centralise all credentials and targeting config in one place.
# - A paginated GET loop hits https://app.asana.com/api/1.0/projects, pulling
#   up to 100 records per page via Bearer token auth. opt_fields restricts the
#   payload to only the fields the script needs (parent.name, assignee.name,
#   start_on, due_on, name, completed_on). On each iteration the response is
#   validated (raise_for_status), its "data" array is appended to the master
#   tasks list, and the loop continues only if a "next_page" offset is present.
# - Once all pages are collected, each task is flattened into a row dict.
#   The nested assignee object is resolved safely with a fallback empty dict
#   to handle unassigned tasks without error. Fields are mapped to task_id,
#   task_name, completed, due_date, assignee, and name, then passed to
#   pd.DataFrame() to produce a clean, column-oriented structure for
#   downstream filtering and reporting.
# ──────────────────────────────────────────────────────────────────────────────


ASANA_TOKEN  = ""
GROUP_ID = ""
REPORT_RECIPIENT = ""

url = f"https://app.asana.com/api/1.0/projects/{GROUP_ID}/tasks"

headers = {"Authorization": f"Bearer {ASANA_TOKEN}"}
params = {"limit": 100, 
          "opt_fields":
          "parent.name,assignee.name,start_on,due_on, name, completed_on"          
          }
tasks = []

while True:
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    result = response.json()
    tasks.extend(result["data"])
    next_page = result.get("next_page")
    if not next_page: 
        break
    params["offset"] = next_page["offset"]
    
rows = []
for task in tasks:
    assignee = task.get("assignee") or {}
    rows.append({"task_id": task.get("gid"),
                 "task_name": task.get("name"),
                 "completed": task.get("completed"),
                 "due_date": task.get("due_on"),
                 "assignee": assignee.get("name"),
                 "name":task.get("name")
                })
df= pd.DataFrame(rows)


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
