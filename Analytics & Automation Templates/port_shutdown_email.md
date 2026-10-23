# Asana Task Summary Reporter

Pulls tasks from the Asana API, filters for incomplete and overdue work,
exports to Excel, and sends an HTML summary report via Outlook.

---

## Requirements

Python 3.8+ and Windows with Microsoft Outlook installed.

pip install pandas numpy requests pywin32

---

## Configuration

Fill in the following constants at the top of `asana_report.py` before running:

| Constant               | Description                                      |
|------------------------|--------------------------------------------------|
| `ASANA_TOKEN`          | Your Asana Personal Access Token                 |
| `GROUP_ID`             | Target Asana workspace or team ID                |
| `REPORT_RECIPIENT`     | Primary report recipient email                   |
| `RECIPIENT_EMAIL`      | Outlook To: field                                |
| `CC_EMAIL`             | Outlook CC: field                                |
| `SENDER_NAME`          | Name used in the email sign-off                  |
| `LOOKBACK_DAYS`        | How many days back to scan (e.g. 7)              |
| `PARENT_NAME_MIN_LEN`  | Min parent task name length to filter noise      |

---

## Usage

python asana_report.py

Outputs `incomplete_tasks.xlsx` and `overdue_tasks.xlsx` to the working
directory, then dispatches the HTML email via Outlook.

---

## .gitignore

*.xlsx
*.csv
__pycache__/
*.pyc

---

## Author
Adewole Aboyade-Cole — Amazon Global Realty Tech
