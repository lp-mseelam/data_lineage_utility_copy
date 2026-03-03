import os
import pandas as pd
from llm_call import safe_gemini_call_for_repo, DEFAULT_REPO_PATH, log


def process_llm_output(file_path: str, repo_path: str = None):
    """
    Run lineage extraction and export to Excel with two sheets:
      - 'lineage'
      - 'quality_issues'

    Parameters
    ==========
    file_path: str
        Location of file where llm result in excel format should be dumped.
    repo_path: str
        Repo Path for which Data Lineage is to be created;
    """
    try:
        repo_path = repo_path or DEFAULT_REPO_PATH
        payload = safe_gemini_call_for_repo(repo_path)

        if payload is None:
            log("No lineage extracted (LLM call returned None).", "ERROR")
            return

        lineage = payload.get("lineage", [])
        issues = payload.get("quality_issues", [])

        os.makedirs(os.path.dirname(os.path.abspath(file_path)) or ".", exist_ok=True)

        with pd.ExcelWriter(file_path, engine="openpyxl") as xlw:
            pd.DataFrame(lineage).to_excel(xlw, sheet_name="lineage", index=False)
            pd.DataFrame({"issue": issues if isinstance(issues, list) else []}) \
              .to_excel(xlw, sheet_name="quality_issues", index=False)

        log(f"Exported lineage to {os.path.abspath(file_path)}", "INFO")

    except Exception as e:
        log(f"Failed to process LLM output: {e}", "ERROR")
        raise

if __name__ == "__main__":
    process_llm_output(file_path="data/DataLineage.xlsx")